import asyncio
import json
import os
import threading
import time
import uuid
import webbrowser
from pathlib import Path
from typing import Dict

from fastapi import (
    BackgroundTasks,
    FastAPI,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.agent.manus import Manus
from app.flow.base import FlowType
from app.flow.flow_factory import FlowFactory
from app.web.log_handler import capture_session_logs, get_logs
from app.web.log_parser import get_all_logs_info, get_latest_log_info, parse_log_file
from app.web.thinking_tracker import ThinkingTracker


# Controls whether to automatically open the browser (reads environment variable, defaults to True)
AUTO_OPEN_BROWSER = os.environ.get("AUTO_OPEN_BROWSER", "1") == "1"
last_opened = False  # Tracks if the browser has already been opened

app = FastAPI(title="OpenManus Web")

# Get the directory of the current file
current_dir = Path(__file__).parent
# Set the static files directory
app.mount("/static", StaticFiles(directory=current_dir / "static"), name="static")
# Set the templates directory
templates = Jinja2Templates(directory=current_dir / "templates")

# Stores active sessions and their results
active_sessions: Dict[str, dict] = {}

# Stores task cancellation events
cancel_events: Dict[str, asyncio.Event] = {}

# Create the workspace root directory
WORKSPACE_ROOT = Path(__file__).parent.parent.parent / "workspace"
WORKSPACE_ROOT.mkdir(exist_ok=True)

# Logs directory
LOGS_DIR = Path(__file__).parent.parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Import the log monitor
from app.utils.log_monitor import LogFileMonitor


# Stores active log monitors
active_log_monitors: Dict[str, LogFileMonitor] = {}


# Function to create a workspace directory
def create_workspace(session_id: str) -> Path:
    """Creates a workspace directory for a session."""
    # Simplify the session_id to use as a directory name
    job_id = f"job_{session_id[:8]}"
    workspace_dir = WORKSPACE_ROOT / job_id
    workspace_dir.mkdir(exist_ok=True)
    return workspace_dir


@app.on_event("startup")
async def startup_event():
    """Startup event: Automatically opens the browser when the application starts."""
    global last_opened
    if AUTO_OPEN_BROWSER and not last_opened:
        # Delay for 1 second to ensure the service has started
        threading.Timer(1.0, lambda: webbrowser.open("http://localhost:8000")).start()
        print("🌐 Automatically opening browser...")
        last_opened = True


class SessionRequest(BaseModel):
    prompt: str


@app.get("/", response_class=HTMLResponse)
async def get_home(request: Request):
    """Homepage endpoint - uses the connected interface."""
    return HTMLResponse(
        content=open(
            current_dir / "static" / "connected_interface.html", encoding="utf-8"
        ).read()
    )


@app.get("/original", response_class=HTMLResponse)
async def get_original_interface(request: Request):
    """Original interface endpoint."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/connected", response_class=HTMLResponse)
async def get_connected_interface(request: Request):
    """New interface endpoint connected to the backend (same as homepage)."""
    return HTMLResponse(
        content=open(
            current_dir / "static" / "connected_interface.html", encoding="utf-8"
        ).read()
    )


@app.post("/api/chat")
async def create_chat_session(
    session_req: SessionRequest, background_tasks: BackgroundTasks
):
    session_id = str(uuid.uuid4())
    active_sessions[session_id] = {
        "status": "processing",
        "result": None,
        "log": [],
        "workspace": None,
    }

    # Create a cancellation event
    cancel_events[session_id] = asyncio.Event()

    # Create the workspace directory
    workspace_dir = create_workspace(session_id)
    active_sessions[session_id]["workspace"] = str(
        workspace_dir.relative_to(WORKSPACE_ROOT)
    )

    background_tasks.add_task(process_prompt, session_id, session_req.prompt)
    return {
        "session_id": session_id,
        "workspace": active_sessions[session_id]["workspace"],
    }


@app.get("/api/chat/{session_id}")
async def get_chat_result(session_id: str):
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    # Use the new log handling module to get logs
    session = active_sessions[session_id]
    session["log"] = get_logs(session_id)

    return session


@app.post("/api/chat/{session_id}/stop")
async def stop_processing(session_id: str):
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    if session_id in cancel_events:
        cancel_events[session_id].set()

    active_sessions[session_id]["status"] = "stopped"
    active_sessions[session_id]["result"] = "Processing was stopped by the user"

    return {"status": "stopped"}


@app.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    try:
        await websocket.accept()

        if session_id not in active_sessions:
            await websocket.send_text(json.dumps({"error": "Session not found"}))
            await websocket.close()
            return

        session = active_sessions[session_id]

        # Register the WebSocket sending callback function
        async def ws_send(message: str):
            try:
                await websocket.send_text(message)
            except Exception as e:
                print(f"Failed to send WebSocket message: {str(e)}")

        ThinkingTracker.register_ws_send_callback(session_id, ws_send)

        # Add log information to the initial status notification
        await websocket.send_text(
            json.dumps(
                {
                    "status": session["status"],
                    "log": session["log"],
                    "thinking_steps": ThinkingTracker.get_thinking_steps(session_id),
                    "logs": ThinkingTracker.get_logs(session_id),  # Add log information
                }
            )
        )

        # Get the workspace name (job_id) - prioritize environment variable
        job_id = None
        # First, check the current session's workspace association
        if "workspace" in session:
            job_id = session["workspace"]

        # If there's no log monitor currently, create one
        if session_id not in active_log_monitors and job_id:
            log_path = LOGS_DIR / f"{job_id}.log"
            if log_path.exists():
                log_monitor = LogFileMonitor(job_id)
                log_monitor.start_monitoring()
                active_log_monitors[session_id] = log_monitor

        # Track log updates
        last_log_entries = []
        if job_id and session_id in active_log_monitors:
            last_log_entries = active_log_monitors[session_id].get_log_entries()

        # Wait for result updates
        last_log_count = 0
        last_thinking_step_count = 0
        last_tracker_log_count = 0  # Add ThinkingTracker log count

        while session["status"] == "processing":
            await asyncio.sleep(0.2)  # Reduce the check interval to improve real-time performance

            # Check for system log updates (new)
            if job_id and session_id in active_log_monitors:
                current_log_entries = active_log_monitors[session_id].get_log_entries()
                if len(current_log_entries) > len(last_log_entries):
                    new_logs = current_log_entries[len(last_log_entries) :]
                    await websocket.send_text(
                        json.dumps(
                            {
                                "status": session["status"],
                                "system_logs": new_logs,
                                # Add a chat_logs field to send system logs as chat messages
                                "chat_logs": new_logs,
                            }
                        )
                    )
                    last_log_entries = current_log_entries

            # Check for log updates
            current_log_count = len(session["log"])
            if current_log_count > last_log_count:
                await websocket.send_text(
                    json.dumps(
                        {
                            "status": session["status"],
                            "log": session["log"][last_log_count:],
                        }
                    )
                )
                last_log_count = current_log_count

            # Check for thinking step updates
            thinking_steps = ThinkingTracker.get_thinking_steps(session_id)
            current_thinking_step_count = len(thinking_steps)
            if current_thinking_step_count > last_thinking_step_count:
                await websocket.send_text(
                    json.dumps(
                        {
                            "status": session["status"],
                            "thinking_steps": thinking_steps[last_thinking_step_count:],
                        }
                    )
                )
                last_thinking_step_count = current_thinking_step_count

            # Check for ThinkingTracker log updates
            tracker_logs = ThinkingTracker.get_logs(session_id)
            current_tracker_log_count = len(tracker_logs)
            if current_tracker_log_count > last_tracker_log_count:
                await websocket.send_text(
                    json.dumps(
                        {
                            "status": session["status"],
                            "logs": tracker_logs[last_tracker_log_count:],
                        }
                    )
                )
                last_tracker_log_count = current_tracker_log_count

            # Check for result updates
            if session["result"]:
                await websocket.send_text(
                    json.dumps(
                        {
                            "status": session["status"],
                            "result": session["result"],
                            "log": session["log"][last_log_count:],
                            "thinking_steps": ThinkingTracker.get_thinking_steps(
                                session_id, last_thinking_step_count
                            ),
                            "system_logs": last_log_entries,  # Add system logs
                            "logs": ThinkingTracker.get_logs(
                                session_id, last_tracker_log_count
                            ),  # Add ThinkingTracker logs
                        }
                    )
                )
                break  # Result has been sent, exit the loop to avoid duplicate sending

        # Only send the final result if the loop didn't break due to a result
        if not session["result"]:
            await websocket.send_text(
                json.dumps(
                    {
                        "status": session["status"],
                        "result": session["result"],
                        "log": session["log"][last_log_count:],
                        "thinking_steps": ThinkingTracker.get_thinking_steps(
                            session_id, last_thinking_step_count
                        ),
                        "system_logs": last_log_entries,  # Add system logs
                        "logs": ThinkingTracker.get_logs(
                            session_id, last_tracker_log_count
                        ),  # Add ThinkingTracker logs
                    }
                )
            )

        # Unregister the WebSocket sending callback function
        ThinkingTracker.unregister_ws_send_callback(session_id)
        await websocket.close()
    except WebSocketDisconnect:
        # Client disconnected, normal operation
        ThinkingTracker.unregister_ws_send_callback(session_id)
    except Exception as e:
        # Other exceptions, log the error but don't interrupt the application
        print(f"WebSocket error: {str(e)}")
        ThinkingTracker.unregister_ws_send_callback(session_id)


# Add LLM communication hooks at the appropriate place
from app.web.thinking_tracker import ThinkingTracker


# Modify the communication tracker's implementation method
class LLMCommunicationTracker:
    """Tracks communication with the LLM, using monkey-patching instead of callbacks."""

    def __init__(self, session_id: str, agent=None):
        self.session_id = session_id
        self.agent = agent
        self.original_run_method = None

        # If an agent is provided, install hooks
        if agent and hasattr(agent, "llm") and hasattr(agent.llm, "completion"):
            self.install_hooks()

    def install_hooks(self):
        """Installs hooks to capture LLM communication content."""
        if not self.agent or not hasattr(self.agent, "llm"):
            return False

        # Save the original method
        llm = self.agent.llm
        if hasattr(llm, "completion"):
            self.original_completion = llm.completion
            # Replace with our wrapper method
            llm.completion = self._wrap_completion(self.original_completion)
            return True
        return False

    def uninstall_hooks(self):
        """Uninstalls hooks, restoring the original method."""
        if self.agent and hasattr(self.agent, "llm") and self.original_completion:
            self.agent.llm.completion = self.original_completion

    def _wrap_completion(self, original_method):
        """Wraps the LLM's completion method to capture input and output."""
        session_id = self.session_id

        async def wrapped_completion(*args, **kwargs):
            # Record input
            prompt = kwargs.get("prompt", "")
            if not prompt and args:
                prompt = args[0]
            if prompt:
                ThinkingTracker.add_communication(
                    session_id,
                    "Sending to LLM",
                    prompt[:500] + ("..." if len(prompt) > 500 else ""),
                )

            # Call the original method
            result = await original_method(*args, **kwargs)

            # Record output
            if result:
                content = result
                if isinstance(result, dict) and "content" in result:
                    content = result["content"]
                elif hasattr(result, "content"):
                    content = result.content

                if isinstance(content, str):
                    ThinkingTracker.add_communication(
                        session_id,
                        "Receiving from LLM",
                        content[:500] + ("..." if len(content) > 500 else ""),
                    )

            return result

        return wrapped_completion


# Import the newly created LLM wrapper
from app.agent.llm_wrapper import LLMCallbackWrapper


# Modify file API to support workspace directories
@app.get("/api/files")
async def get_generated_files():
    """Get all workspace directories and files."""
    result = []

    # Get all workspace directories
    workspaces = list(WORKSPACE_ROOT.glob("job_*"))
    workspaces.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    for workspace in workspaces:
        workspace_name = workspace.name
        # Get all files within the workspace and sort by modification time
        files = []
        with os.scandir(workspace) as it:
            for entry in it:
                if entry.is_file() and entry.name.split(".")[-1] in [
                    "txt",
                    "md",
                    "html",
                    "css",
                    "js",
                    "py",
                    "json",
                ]:
                    files.append(entry)
        # Sort by modification time in descending order
        files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

        # If there are files, add this workspace
        if files:
            workspace_item = {
                "name": workspace_name,
                "path": str(workspace.relative_to(Path(__file__).parent.parent.parent)),
                "modified": workspace.stat().st_mtime,
                "files": [],
            }

            # Add files under the workspace
            for file in sorted(files, key=lambda p: p.name):
                workspace_item["files"].append(
                    {
                        "name": file.name,
                        "path": str(
                            Path(file.path).relative_to(
                                Path(__file__).parent.parent.parent
                            )
                        ),
                        "type": Path(file.path).suffix[1:],  # Remove the dot from the extension
                        "size": file.stat().st_size,
                        "modified": file.stat().st_mtime,
                    }
                )

            result.append(workspace_item)

    return {"workspaces": result}


# New log file API
@app.get("/api/logs")
async def get_system_logs(limit: int = 10):
    """Get the list of system logs."""
    log_files = []
    for entry in os.scandir(LOGS_DIR):
        if entry.is_file() and entry.name.endswith(".log"):
            log_files.append(
                {
                    "name": entry.name,
                    "size": entry.stat().st_size,
                    "modified": entry.stat().st_mtime,
                }
            )
    # Sort by modification time in descending order and limit the number
    log_files.sort(key=lambda x: x["modified"], reverse=True)
    return {"logs": log_files[:limit]}


@app.get("/api/logs/{log_name}")
async def get_log_content(log_name: str, parsed: bool = False):
    """Get the content of a specific log file."""
    log_path = LOGS_DIR / log_name
    # Security check
    if not log_path.exists() or not log_path.is_file():
        raise HTTPException(status_code=404, detail="Log file not found")

    # If parsed log information is requested
    if parsed:
        log_info = parse_log_file(str(log_path))
        log_info["name"] = log_name
        return log_info

    # Otherwise, return the raw content
    with open(log_path, "r", encoding="utf-8") as f:
        content = f.read()

    return {"name": log_name, "content": content}


@app.get("/api/logs_parsed")
async def get_parsed_logs(limit: int = 10):
    """Get a list of parsed log information."""
    return {"logs": get_all_logs_info(str(LOGS_DIR), limit)}


@app.get("/api/logs_parsed/{log_name}")
async def get_parsed_log(log_name: str):
    """Get the parsed information for a specific log file."""
    log_path = LOGS_DIR / log_name
    # Security check
    if not log_path.exists() or not log_path.is_file():
        raise HTTPException(status_code=404, detail="Log file not found")

    log_info = parse_log_file(str(log_path))
    log_info["name"] = log_name
    return log_info


@app.get("/api/latest_log")
async def get_latest_log():
    """Get the parsed information for the latest log file."""
    return get_latest_log_info(str(LOGS_DIR))


@app.get("/api/files/{file_path:path}")
async def get_file_content(file_path: str):
    """Get the content of a specific file."""
    # Security check to prevent directory traversal attacks
    root_dir = Path(__file__).parent.parent.parent
    full_path = root_dir / file_path

    # Ensure the file is within the project directory
    try:
        full_path.relative_to(root_dir)
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")

    if not full_path.exists() or not full_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    # Read file content
    try:
        with open(full_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Determine file type
        file_type = full_path.suffix[1:] if full_path.suffix else "text"

        return {
            "name": full_path.name,
            "path": file_path,
            "type": file_type,
            "content": content,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")


# Modify the process_prompt function to handle workspaces
async def process_prompt(session_id: str, prompt: str):
    # Get the session workspace
    workspace_dir = None
    if session_id in active_sessions and "workspace" in active_sessions[session_id]:
        workspace_path = active_sessions[session_id]["workspace"]
        workspace_dir = WORKSPACE_ROOT / workspace_path
        os.makedirs(workspace_dir, exist_ok=True)

    # If there is no workspace, create one
    if not workspace_dir:
        workspace_dir = create_workspace(session_id)
        if session_id in active_sessions:
            active_sessions[session_id]["workspace"] = str(
                workspace_dir.relative_to(WORKSPACE_ROOT)
            )

    # Set the current working directory to the workspace
    original_cwd = os.getcwd()
    os.chdir(workspace_dir)

    # Use the workspace name as the prefix for the log file name
    job_id = workspace_dir.name
    # Set the log file path
    task_log_path = LOGS_DIR / f"{job_id}.log"

    # Create a log monitor and start monitoring
    log_monitor = LogFileMonitor(job_id)
    observer = log_monitor.start_monitoring()
    active_log_monitors[session_id] = log_monitor

    async def sync_logs():
        """Periodically fetch logs from LogFileMonitor and update ThinkingTracker in real-time."""
        last_count = 0
        try:
            while True:
                if session_id not in active_log_monitors:
                    break
                current_logs = active_log_monitors[session_id].get_log_entries()
                if len(current_logs) > last_count:
                    # Process new log entries
                    new_logs = current_logs[last_count:]
                    # Process each new log entry individually to ensure real-time updates
                    for log_entry in new_logs:
                        # Process each log individually, adding it immediately to ThinkingTracker
                        ThinkingTracker.add_log_entry(
                            session_id,
                            {
                                "level": log_entry.get("level", "INFO"),
                                "message": log_entry.get("message", ""),
                                "timestamp": log_entry.get("timestamp", time.time()),
                            },
                        )
                    last_count = len(current_logs)
                # Reduce polling interval to improve real-time performance
                await asyncio.sleep(0.1)  # Check every 0.1 seconds
        except Exception as e:
            print(f"An error occurred while syncing logs: {str(e)}")

    # Start the log synchronization task
    sync_task = asyncio.create_task(sync_logs())

    # Set environment variables to tell the logger to use this log file, ensuring both methods are set
    os.environ["OPENMANUS_LOG_FILE"] = str(task_log_path)
    os.environ["OPENMANUS_TASK_ID"] = job_id

    try:
        # Use the log capture context manager to parse log level and content
        with capture_session_logs(session_id) as log:
            # Initialize thinking tracking
            ThinkingTracker.start_tracking(session_id)
            ThinkingTracker.add_thinking_step(session_id, "Starting to process user request")
            ThinkingTracker.add_thinking_step(
                session_id, f"Workspace directory: {workspace_dir.name}"
            )

            # Directly record the user's input prompt
            ThinkingTracker.add_communication(session_id, "User Input", prompt)

            # Initialize the agent and task flow
            ThinkingTracker.add_thinking_step(session_id, "Initializing AI agent and task flow")
            agent = Manus()

            # Wrap the LLM using the wrapper
            if hasattr(agent, "llm"):
                original_llm = agent.llm
                wrapped_llm = LLMCallbackWrapper(original_llm)

                # Register callback functions
                def on_before_request(data):
                    # Extract request content
                    prompt_content = None
                    if data.get("args") and len(data["args"]) > 0:
                        prompt_content = str(data["args"][0])
                    elif data.get("kwargs") and "prompt" in data["kwargs"]:
                        prompt_content = data["kwargs"]["prompt"]
                    else:
                        prompt_content = str(data)

                    # Record communication content
                    print(f"Sending to LLM: {prompt_content[:100]}...")
                    ThinkingTracker.add_communication(
                        session_id, "Sending to LLM", prompt_content
                    )

                def on_after_request(data):
                    # Extract response content
                    response = data.get("response", "")
                    response_content = ""

                    # Try to extract text content from different formats
                    if isinstance(response, str):
                        response_content = response
                    elif isinstance(response, dict):
                        if "content" in response:
                            response_content = response["content"]
                        elif "text" in response:
                            response_content = response["text"]
                        else:
                            response_content = str(response)
                    elif hasattr(response, "content"):
                        response_content = response.content
                    else:
                        response_content = str(response)

                    # Record communication content
                    print(f"Receiving from LLM: {response_content[:100]}...")
                    ThinkingTracker.add_communication(
                        session_id, "Receiving from LLM", response_content
                    )

                # Register callbacks
                wrapped_llm.register_callback("before_request", on_before_request)
                wrapped_llm.register_callback("after_request", on_after_request)

                # Replace the original LLM
                agent.llm = wrapped_llm

            flow = FlowFactory.create_flow(
                flow_type=FlowType.PLANNING,
                agents=agent,
            )

            # Record the start of processing
            ThinkingTracker.add_thinking_step(
                session_id, f"Analyzing user request: {prompt[:50]}{'...' if len(prompt) > 50 else ''}"
            )
            log.info(f"Starting execution: {prompt[:50]}{'...' if len(prompt) > 50 else ''}")

            # Check if the task has been cancelled
            cancel_event = cancel_events.get(session_id)
            if cancel_event and cancel_event.is_set():
                log.warning("Processing was cancelled by the user")
                ThinkingTracker.mark_stopped(session_id)
                active_sessions[session_id]["status"] = "stopped"
                active_sessions[session_id]["result"] = "Processing was stopped by the user"
                return

            # Check for existing files in the workspace before execution
            existing_files = set()
            for ext in ["*.txt", "*.md", "*.html", "*.css", "*.js", "*.py", "*.json"]:
                existing_files.update(f.name for f in workspace_dir.glob(ext))

            # Track the plan creation process
            ThinkingTracker.add_thinking_step(session_id, "Creating task execution plan")
            ThinkingTracker.add_thinking_step(session_id, "Starting to execute task plan")

            # Get the cancellation event to pass to flow.execute
            cancel_event = cancel_events.get(session_id)

            # Initial check, do not execute if already cancelled
            if cancel_event and cancel_event.is_set():
                log.warning("Processing was cancelled by the user")
                ThinkingTracker.mark_stopped(session_id)
                active_sessions[session_id]["status"] = "stopped"
                active_sessions[session_id]["result"] = "Processing was stopped by the user"
                return

            # Execute the actual processing - pass job_id and cancel_event to the flow.execute method
            result = await flow.execute(prompt, job_id, cancel_event)

            # Check for newly generated files after execution
            new_files = set()
            for ext in ["*.txt", "*.md", "*.html", "*.css", "*.js", "*.py", "*.json"]:
                new_files.update(f.name for f in workspace_dir.glob(ext))
            newly_created = new_files - existing_files

            if newly_created:
                files_list = ", ".join(newly_created)
                ThinkingTracker.add_thinking_step(
                    session_id,
                    f"Generated {len(newly_created)} file(s) in workspace {workspace_dir.name}: {files_list}",
                )
                # Also add the file list to the session result
                active_sessions[session_id]["generated_files"] = list(newly_created)

            # Log completion status
            log.info("Processing complete")
            ThinkingTracker.add_conclusion(
                session_id, f"Task processing complete! Results generated in workspace {workspace_dir.name}."
            )

            active_sessions[session_id]["status"] = "completed"
            active_sessions[session_id]["result"] = result
            active_sessions[session_id][
                "thinking_steps"
            ] = ThinkingTracker.get_thinking_steps(session_id)

    except asyncio.CancelledError:
        # Handle cancellation
        print("Processing was cancelled")
        ThinkingTracker.mark_stopped(session_id)
        active_sessions[session_id]["status"] = "stopped"
        active_sessions[session_id]["result"] = "Processing was cancelled"
    except Exception as e:
        # Handle errors
        error_msg = f"Error during processing: {str(e)}"
        print(error_msg)
        ThinkingTracker.add_error(session_id, f"Encountered an error during processing: {str(e)}")
        active_sessions[session_id]["status"] = "error"
        active_sessions[session_id]["result"] = f"An error occurred: {str(e)}"
    finally:
        # Restore the original working directory
        os.chdir(original_cwd)

        # Clear the log file environment variables
        if "OPENMANUS_LOG_FILE" in os.environ:
            del os.environ["OPENMANUS_LOG_FILE"]
        if "OPENMANUS_TASK_ID" in os.environ:
            del os.environ["OPENMANUS_TASK_ID"]

        # Clean up resources
        if (
            "agent" in locals()
            and hasattr(agent, "llm")
            and isinstance(agent.llm, LLMCallbackWrapper)
        ):
            try:
                # Correctly remove the callbacks
                if "on_before_request" in locals():
                    agent.llm._callbacks["before_request"].remove(on_before_request)
                if "on_after_request" in locals():
                    agent.llm._callbacks["after_request"].remove(on_after_request)
            except (ValueError, Exception) as e:
                print(f"Error while cleaning up callbacks: {str(e)}")

        # Clean up the cancellation event
        if session_id in cancel_events:
            del cancel_events[session_id]

        # If the monitor exists, stop monitoring
        if session_id in active_log_monitors:
            observer.stop()
            observer.join(timeout=1)
            del active_log_monitors[session_id]

        # Cancel the log synchronization task
        if 'sync_task' in locals() and sync_task:
            sync_task.cancel()
            try:
                await sync_task
            except asyncio.CancelledError:
                pass


# Add a new API endpoint to get thinking steps
@app.get("/api/thinking/{session_id}")
async def get_thinking_steps(session_id: str, start_index: int = 0):
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    return {
        "status": ThinkingTracker.get_status(session_id),
        "thinking_steps": ThinkingTracker.get_thinking_steps(session_id, start_index),
    }


# Add an API endpoint to get progress information
@app.get("/api/progress/{session_id}")
async def get_progress(session_id: str):
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    return ThinkingTracker.get_progress(session_id)


# Add an API endpoint to get system logs for a specific session
@app.get("/api/systemlogs/{session_id}")
async def get_system_logs_for_session(session_id: str):
    """Get the system logs for a specific session."""
    if session_id not in active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    job_id = None
    if "workspace" in active_sessions[session_id]:
        workspace_path = active_sessions[session_id]["workspace"]
        job_id = workspace_path

    if not job_id:
        return {"logs": []}

    # Use the monitor if it exists
    if session_id in active_log_monitors:
        logs = active_log_monitors[session_id].get_log_entries()
        return {"logs": logs}

    # Otherwise, read the log file directly
    log_path = LOGS_DIR / f"{job_id}.log"
    if not log_path.exists():
        return {"logs": []}

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            logs = [line.strip() for line in f.readlines()]
        return {"logs": logs}
    except Exception as e:
        return {"error": f"Error reading log file: {str(e)}"}
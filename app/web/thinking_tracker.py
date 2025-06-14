"""
Thinking tracker module, implementing a Manus-style task progress logging system
"""
import asyncio
import json
import threading
import time
from enum import Enum
from typing import Any, Dict, List, Optional


# Global thinking step storage
class ThinkingStep:
    """Represents a single thinking step"""

    def __init__(
        self, message: str, step_type: str = "thinking", details: Optional[str] = None
    ):
        self.message = message
        self.step_type = step_type  # thinking, conclusion, error, communication
        self.details = details  # Used to store communication content or detailed information
        self.timestamp = time.time()


class TaskStatus(Enum):
    """Task status enumeration"""

    PENDING = "pending"
    THINKING = "thinking"
    COMPLETED = "completed"
    ERROR = "error"
    STOPPED = "stopped"


class ThinkingTracker:
    """Thinking tracker, used to record and manage the AI's thinking process"""

    # Class variables to store thinking steps, status, progress, and logs for all sessions
    _session_steps: Dict[str, List[ThinkingStep]] = {}
    _session_status: Dict[str, TaskStatus] = {}
    _session_progress: Dict[str, Dict[str, Any]] = {}  # Stores progress information
    _session_logs: Dict[str, List[Dict]] = {}  # Stores log content
    _ws_send_callbacks: Dict[str, Any] = {}  # Stores WebSocket sending callback functions
    _lock = threading.Lock()

    @classmethod
    def register_ws_send_callback(cls, session_id: str, callback: Any) -> None:
        """Register a WebSocket sending callback function"""
        with cls._lock:
            cls._ws_send_callbacks[session_id] = callback

    @classmethod
    def unregister_ws_send_callback(cls, session_id: str) -> None:
        """Unregister a WebSocket sending callback function"""
        with cls._lock:
            if session_id in cls._ws_send_callbacks:
                del cls._ws_send_callbacks[session_id]

    @classmethod
    def start_tracking(cls, session_id: str) -> None:
        """Start tracking the thinking process for a session"""
        with cls._lock:
            cls._session_steps[session_id] = []
            cls._session_status[session_id] = TaskStatus.THINKING
            cls._session_progress[session_id] = {
                "current_step": "Initializing",
                "total_steps": 0,
                "completed_steps": 0,
                "percentage": 0,
            }

    @classmethod
    def add_thinking_step(
        cls, session_id: str, message: str, details: Optional[str] = None
    ) -> None:
        """Add a thinking step"""
        step = ThinkingStep(message, "thinking", details)
        with cls._lock:
            if session_id in cls._session_steps:
                cls._session_steps[session_id].append(step)

                # Update progress information
                if session_id in cls._session_progress:
                    progress = cls._session_progress[session_id]
                    progress["current_step"] = message

                    # Attempt to extract step number and total steps from the message
                    import re

                    match = re.search(r"Executing step (\d+)/(\d+)", message)
                    if match:
                        current_step_num = int(match.group(1))
                        total_steps = int(match.group(2))
                        progress["total_steps"] = total_steps
                        progress["completed_steps"] = (
                            current_step_num - 1
                        )  # Mark previous as complete

                    if progress["total_steps"] > 0:
                        progress["percentage"] = min(
                            int(
                                100
                                * progress["completed_steps"]
                                / progress["total_steps"]
                            ),
                            99,
                        )

    @classmethod
    def add_communication(cls, session_id: str, direction: str, content: str) -> None:
        """Add a communication record

        Args:
            session_id: The session ID.
            direction: Communication direction, e.g., "Sending to LLM", "Receiving from LLM".
            content: The communication content.
        """
        message = f"{direction} communication"
        step = ThinkingStep(message, "communication", content)
        with cls._lock:
            if session_id in cls._session_steps:
                cls._session_steps[session_id].append(step)

    @classmethod
    def update_progress(
        cls, session_id: str, total_steps: int = None, current_step: str = None
    ):
        """Update task progress information"""
        with cls._lock:
            if session_id in cls._session_progress:
                progress = cls._session_progress[session_id]

                if total_steps is not None:
                    progress["total_steps"] = total_steps

                if current_step is not None:
                    progress["current_step"] = current_step

                # Recalculate percentage
                if progress["total_steps"] > 0:
                    progress["percentage"] = min(
                        int(100 * progress["completed_steps"] / progress["total_steps"]),
                        99,  # Up to 99%, only reaches 100% upon completion
                    )

    @classmethod
    def add_conclusion(
        cls, session_id: str, message: str, details: Optional[str] = None
    ) -> None:
        """Add a conclusion"""
        step = ThinkingStep(message, "conclusion", details)
        with cls._lock:
            if session_id in cls._session_steps:
                cls._session_steps[session_id].append(step)
                cls._session_status[session_id] = TaskStatus.COMPLETED

                # Update progress to 100%
                if session_id in cls._session_progress:
                    progress = cls._session_progress[session_id]
                    progress["percentage"] = 100
                    progress["current_step"] = "Completed"

    @classmethod
    def add_error(cls, session_id: str, message: str) -> None:
        """Add an error message"""
        step = ThinkingStep(message, "error")
        with cls._lock:
            if session_id in cls._session_steps:
                cls._session_steps[session_id].append(step)
                cls._session_status[session_id] = TaskStatus.ERROR

    @classmethod
    def mark_stopped(cls, session_id: str) -> None:
        """Mark the task as stopped"""
        with cls._lock:
            if session_id in cls._session_status:
                cls._session_status[session_id] = TaskStatus.STOPPED

    @classmethod
    def get_thinking_steps(cls, session_id: str, start_index: int = 0) -> List[Dict]:
        """Get the thinking steps for a specified session"""
        with cls._lock:
            if session_id not in cls._session_steps:
                return []

            steps = cls._session_steps[session_id][start_index:]
            return [
                {
                    "message": step.message,
                    "type": step.step_type,
                    "details": step.details,
                    "timestamp": step.timestamp,
                }
                for step in steps
            ]

    @classmethod
    def get_progress(cls, session_id: str) -> Dict[str, Any]:
        """Get task progress information"""
        with cls._lock:
            if session_id not in cls._session_progress:
                return {
                    "current_step": "Not started",
                    "total_steps": 0,
                    "completed_steps": 0,
                    "percentage": 0,
                }
            return cls._session_progress[session_id].copy()

    @classmethod
    def get_status(cls, session_id: str) -> str:
        """Get the task status"""
        with cls._lock:
            if session_id not in cls._session_status:
                return TaskStatus.PENDING.value
            return cls._session_status[session_id].value

    @classmethod
    def clear_session(cls, session_id: str) -> None:
        """Clear the records for a specified session"""
        with cls._lock:
            if session_id in cls._session_steps:
                del cls._session_steps[session_id]
            if session_id in cls._session_status:
                del cls._session_status[session_id]
            if session_id in cls._session_progress:
                del cls._session_progress[session_id]
            if session_id in cls._session_logs:
                del cls._session_logs[session_id]

    @classmethod
    def add_log_entry(cls, session_id: str, entry: Dict) -> None:
        """Add a log entry"""
        with cls._lock:
            if session_id not in cls._session_logs:
                cls._session_logs[session_id] = []

            # Ensure the log entry has the necessary fields
            if "timestamp" not in entry:
                entry["timestamp"] = time.time()

            cls._session_logs[session_id].append(entry)

            # Automatically add corresponding thinking steps based on the log level,
            # processing log content in more detail
            msg = entry.get("message", "")
            if entry.get("level") == "INFO":
                # Generate more meaningful thinking steps for specific types of log content
                if "Starting execution" in msg:
                    cls.add_thinking_step(
                        session_id, f"Starting task: {msg.replace('Starting execution: ', '')}"
                    )
                elif "Executing step" in msg or "Step" in msg:
                    # Try to extract step information
                    cls.add_thinking_step(session_id, f"Executing: {msg}")
                elif "Completed" in msg or "Success" in msg:
                    cls.add_thinking_step(session_id, f"Completed: {msg}")
                else:
                    cls.add_thinking_step(session_id, f"Info: {msg}")
            elif entry.get("level") == "ERROR":
                cls.add_error(session_id, f"Error: {msg}")
            elif entry.get("level") == "WARNING":
                cls.add_thinking_step(session_id, f"Warning: {msg}", "warning")

            # Identify progress information and update
            cls._update_progress_from_log(session_id, msg)

        # Immediately notify the WebSocket client after adding a log
        cls._notify_ws_log_update(session_id, entry)

    @classmethod
    def _notify_ws_log_update(cls, session_id: str, log_entry: Dict):
        """Notify the WebSocket client that there is a new log entry"""
        with cls._lock:
            if session_id in cls._ws_send_callbacks:
                callback = cls._ws_send_callbacks[session_id]
                try:
                    # Use asyncio.create_task to ensure it's non-blocking
                    asyncio.create_task(
                        callback(
                            json.dumps(
                                {
                                    "status": cls.get_status(session_id),
                                    "logs": [log_entry],
                                }
                            )
                        )
                    )
                except Exception as e:
                    print(f"WebSocket send callback failed: {str(e)}")

    @classmethod
    def _update_progress_from_log(cls, session_id: str, message: str):
        """Extract progress information from the log message and update"""
        import re

        # Try to extract step information from the log
        step_match = re.search(r"Step (\d+)/(\d+)", message)
        if step_match and session_id in cls._session_progress:
            current_step = int(step_match.group(1))
            total_steps = int(step_match.group(2))
            progress = cls._session_progress[session_id]

            progress["current_step"] = message
            progress["total_steps"] = total_steps
            progress["completed_steps"] = current_step - 1

            # Recalculate percentage
            if total_steps > 0:
                progress["percentage"] = min(
                    int(100 * progress["completed_steps"] / total_steps),
                    99,  # Up to 99%, only reaches 100% upon completion
                )

    @classmethod
    def add_log_entries(cls, session_id: str, entries: List[Dict]) -> None:
        """Add log entries in bulk"""
        for entry in entries:
            cls.add_log_entry(session_id, entry)

    @classmethod
    def get_logs(cls, session_id: str, start_index: int = 0) -> List[Dict]:
        """Get log entries for a specified session"""
        with cls._lock:
            if session_id not in cls._session_logs:
                return []
            return cls._session_logs[session_id][start_index:]


# Predefined thinking step templates
RESEARCH_STEPS = [
    "Analyze problem requirements and context",
    "Determine search keywords",
    "Retrieve relevant knowledge bases and materials",
    "Analyze and organize the retrieved information",
    "Evaluate feasible solutions",
    "Integrate information and build a solution framework",
    "Generate the final answer",
]

CODING_STEPS = [
    "Analyze code requirements and functional specifications",
    "Design code structure and interfaces",
    "Develop core algorithm logic",
    "Write main functional modules",
    "Implement edge cases and error handling",
    "Perform code testing and debugging",
    "Optimize code performance and readability",
    "Finalize code and documentation",
]

WRITING_STEPS = [
    "Gathering materials related to the writing topic...",
    "Brainstorming the content outline and key points...",
    "Drafting the initial content...",
    "Refining arguments and fact-checking...",
    "Polishing language and formatting...",
]

# Mapping of task types to predefined steps
TASK_TYPE_STEPS = {
    "research": RESEARCH_STEPS,
    "coding": CODING_STEPS,
    "writing": WRITING_STEPS,
}


def generate_thinking_steps(
    session_id: str,
    task_type: str = "research",
    task_description: str = "",
    show_communication: bool = True,
) -> None:
    """Generate a series of thinking steps to simulate the AI's thinking process"""
    steps = TASK_TYPE_STEPS.get(task_type, RESEARCH_STEPS)

    # If there is a description, add more specific steps
    if task_description:
        specific_steps = [
            f"Researching information about {task_description}",
            f"Analyzing the key points of {task_description}",
            f"Organizing a solution for {task_description}",
        ]
        steps = specific_steps + steps

    ThinkingTracker.start_tracking(session_id)
    ThinkingTracker.update_progress(
        session_id, total_steps=len(steps) + 2
    )  # +2 for the start and end steps

    # Add initial step
    ThinkingTracker.add_thinking_step(
        session_id, f"Start processing task: {task_description if task_description else 'New request'}"
    )

    # Simulate adding a thinking step at regular intervals
    for step in steps:
        ThinkingTracker.add_thinking_step(session_id, step)

        # Simulate communication with the LLM
        if show_communication:
            # Simulate sending a request to the LLM
            ThinkingTracker.add_communication(session_id, "Sending to LLM", f"Please help me {step}...")

            # Simulate receiving a reply from the LLM
            ThinkingTracker.add_communication(
                session_id, "Receiving from LLM", f"I have completed {step}. Here are the relevant results: [Detailed information]"
            )

    # Add conclusion
    ThinkingTracker.add_conclusion(session_id, "Task processing complete! Results have been generated.")
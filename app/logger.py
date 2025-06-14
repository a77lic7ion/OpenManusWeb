import os
import sys
import time
from pathlib import Path

from loguru import logger


# Get the project root directory
project_root = Path(__file__).parent.parent

# Create the logs directory
logs_dir = project_root / "logs"
logs_dir.mkdir(exist_ok=True)

# Check if a log file is specified
log_file = os.environ.get("OPENMANUS_LOG_FILE")

if not log_file:
    # If not specified, check for a task ID (from session or workspace directory name)
    task_id = os.environ.get("OPENMANUS_TASK_ID", "")

    # Use the task ID as the log filename, instead of a date-time format
    if task_id:
        # Ensure the task ID starts with "job_"
        if not task_id.startswith("job_"):
            task_id = f"job_{task_id}"
        log_filename = f"{task_id}.log"
    else:
        # If there is no task ID, create a job_ID format log filename using a timestamp
        job_id = f"job_{int(time.time())}"
        log_filename = f"{job_id}.log"

    log_file = logs_dir / log_filename
else:
    # Use the specified log file
    log_file = Path(log_file)

# Configure loguru logger
logger.remove()  # Remove the default handler
# Add console output
logger.add(
    sys.stderr,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="INFO",
)
# Add file output
logger.add(
    log_file,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="INFO",
    rotation="100 MB",
    retention="10 days",
)

# Export the configured logger
__all__ = ["logger"]

if __name__ == "__main__":
    logger.info("Starting application")
    logger.debug("Debug message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")

    try:
        raise ValueError("Test error")
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
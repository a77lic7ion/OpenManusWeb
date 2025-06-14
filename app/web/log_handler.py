"""
Simple log processing module for web application log capture.
"""
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List

from loguru import logger


# Global log storage
session_logs: Dict[str, List[Dict]] = {}
_lock = threading.Lock()


# Register a custom log handler to store logs categorized by session ID
class SessionLogHandler:
    def __init__(self, session_id: str):
        self.session_id = session_id

    def __call__(self, record):
        log_entry = {
            # "time": record["time"].strftime("%Y-%m-%d %H:%M:%S.%f"),
            # "level": record["level"].name,
            "message": record,
            "timestamp": datetime.now().timestamp(),
        }

        with _lock:
            if self.session_id not in session_logs:
                session_logs[self.session_id] = []
            session_logs[self.session_id].append(log_entry)

        # Pass the record to continue the processing chain
        return True


class SimpleLogCapture:
    """A simple log capturer that provides a logger-like interface."""

    def __init__(self, session_id: str):
        self.session_id = session_id

    def info(self, message: str) -> None:
        """Log an info-level message."""
        add_log(self.session_id, "INFO", message)
        logger.info(message)

    def warning(self, message: str) -> None:
        """Log a warning-level message."""
        add_log(self.session_id, "WARNING", message)
        logger.warning(message)

    def error(self, message: str) -> None:
        """Log an error-level message."""
        add_log(self.session_id, "ERROR", message)
        logger.error(message)

    def debug(self, message: str) -> None:
        """Log a debug-level message."""
        add_log(self.session_id, "DEBUG", message)
        logger.debug(message)

    def exception(self, message: str) -> None:
        """Log an exception-level message."""
        add_log(self.session_id, "ERROR", message)
        logger.exception(message)


@contextmanager
def capture_session_logs(session_id: str):
    """
    Context manager to capture logs for a specific session.
    Returns a SimpleLogCapture instance instead of the log list directly.
    """
    # Create log storage for this session
    with _lock:
        if session_id not in session_logs:
            session_logs[session_id] = []

    # Add the session-specific log handler
    handler_id = logger.add(SessionLogHandler(session_id))

    # Create a simple log capturer
    log_capture = SimpleLogCapture(session_id)

    try:
        # Return the log capturer instead of the log list
        yield log_capture
    finally:
        # Remove the temporarily added handler
        logger.remove(handler_id)


def add_log(session_id: str, level: str, message: str) -> None:
    """Add a log to a specific session."""
    with _lock:
        if session_id not in session_logs:
            session_logs[session_id] = []

        session_logs[session_id].append(
            {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "level": level,
                "message": message,
                "timestamp": datetime.now().timestamp(),
            }
        )


def get_logs(session_id: str) -> List[Dict]:
    """Get logs for a specific session."""
    with _lock:
        return session_logs.get(session_id, [])[:]


def clear_logs(session_id: str) -> None:
    """Clear logs for a specific session."""
    with _lock:
        if session_id in session_logs:
            session_logs[session_id] = []
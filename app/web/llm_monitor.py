"""
LLM Communication Monitoring Module, used to capture and simulate communication content with an LLM.
"""
import asyncio
import random
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional


class LLMMonitor:
    """LLM Communication Monitor, supports various methods for tracking LLM communication."""

    def __init__(self):
        self.interceptors = []
        self.communications = []

    def register_interceptor(self, func: Callable):
        """Register an interceptor function, which will be called on every communication."""
        self.interceptors.append(func)
        return func  # Convenient for use as a decorator

    def record_communication(self, direction: str, content: Any):
        """Record communication content."""
        comm_record = {
            "direction": direction,  # "in" or "out"
            "content": str(content)[:1000],  # Limit length
            "timestamp": time.time(),
        }
        self.communications.append(comm_record)

        # Notify all interceptors
        for interceptor in self.interceptors:
            try:
                interceptor(comm_record)
            except Exception as e:
                print(f"Interceptor error: {str(e)}")

    def get_communications(self, start_idx: int = 0) -> List[Dict[str, Any]]:
        """Get communication records."""
        return self.communications[start_idx:]

    def clear(self):
        """Clear all communication records."""
        self.communications = []

    def intercept_method(self, obj, method_name):
        """Intercept an object's method call."""
        if not hasattr(obj, method_name):
            return False

        original_method = getattr(obj, method_name)

        @wraps(original_method)
        async def wrapped_method(*args, **kwargs):
            # Record input
            input_data = str(args[0]) if args else str(kwargs)
            self.record_communication("in", input_data)

            # Call the original method
            result = await original_method(*args, **kwargs)

            # Record output
            self.record_communication("out", result)
            return result

        # Replace the original method
        setattr(obj, method_name, wrapped_method)
        return True


# Create a global monitor instance
monitor = LLMMonitor()


# Provide some functions to simulate an LLM, which can be used for demonstration or testing
async def simulate_llm_thinking(
    prompt: str, callback: Optional[Callable] = None, steps: int = 5, delay: float = 1.0
):
    """Simulate the LLM's thinking process, generating a series of thinking steps."""

    # Record input
    monitor.record_communication("in", prompt)

    thinking_steps = [
        "Analyzing problem requirements",
        "Retrieving relevant knowledge",
        "Organizing and structuring information",
        "Drafting a preliminary answer",
        "Reviewing and optimizing the answer",
        "Generating the final response",
    ]

    # Adjust thinking steps based on the prompt
    if "code" in prompt or "programming" in prompt:
        thinking_steps = [
            "Understanding code requirements",
            "Designing code structure",
            "Writing core functions",
            "Implementing error handling",
            "Testing code functionality",
            "Optimizing code efficiency",
        ]

    # Ensure the number of steps is reasonable
    actual_steps = min(steps, len(thinking_steps))

    # Simulate the thinking process
    for i in range(actual_steps):
        step_msg = thinking_steps[i]
        if callback:
            callback(step_msg)
        await asyncio.sleep(delay * (0.5 + random.random()))

    # Generate the answer
    result = f"This is the answer to the question '{prompt[:30]}...'.\n\n"
    result += "Based on my analysis, here are a few suggestions:\n"
    result += "1. First, confirm the core of the problem.\n"
    result += "2. Next, analyze possible solutions.\n"
    result += "3. Finally, choose the most appropriate method to implement."

    # Record output
    monitor.record_communication("out", result)
    return result
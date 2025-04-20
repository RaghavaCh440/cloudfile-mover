"""cloudfile-mover package initialisation."""

# Expose the main API at package level for convenience
from .core import move_file

__all__ = ["move_file"]

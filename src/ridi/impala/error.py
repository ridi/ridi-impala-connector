class ImpalaError(Exception):
    def __init__(self, cause):
        self.cause = cause

    def __str__(self):
        return str(self.cause)

__all__ = ["ImpalaError"]

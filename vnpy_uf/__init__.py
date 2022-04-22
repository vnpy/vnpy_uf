import importlib_metadata

from .gateway import UfxGateway


try:
    __version__ = importlib_metadata.version("vnpy_ufx")
except importlib_metadata.PackageNotFoundError:
    __version__ = "dev"
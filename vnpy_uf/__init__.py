import importlib_metadata

from .gateway import UfGateway


try:
    __version__ = importlib_metadata.version("vnpy_uf")
except importlib_metadata.PackageNotFoundError:
    __version__ = "dev"
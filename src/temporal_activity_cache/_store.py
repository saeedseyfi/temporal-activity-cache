"""Cache store backed by fsspec for remote/local storage."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import fsspec


class CacheStore:
    """A key-value cache store backed by any fsspec-compatible filesystem.

    Cache entries are stored as two files per key:

    - ``{prefix}/{fn_name}/{key}.bin`` -- serialized return value
    - ``{prefix}/{fn_name}/{key}.meta.json`` -- metadata (expiration)

    The store is agnostic to the serialization format. It stores and
    retrieves raw bytes; the caller is responsible for serialization.

    Args:
        base_url: Base URL for the cache store. The scheme determines the
            fsspec backend (``gs://`` for GCS, ``s3://`` for S3, etc.).
        **storage_options: Extra keyword arguments passed to
            ``fsspec.filesystem()``.
    """

    def __init__(self, base_url: str, **storage_options: object) -> None:
        self._base_url = base_url.rstrip("/")
        parsed = urlparse(self._base_url)
        self._protocol = parsed.scheme or "file"
        self._base_path = parsed.netloc + parsed.path if parsed.netloc else parsed.path
        self._fs = fsspec.filesystem(self._protocol, **storage_options)

    def _value_path(self, fn_name: str, key: str) -> str:
        return f"{self._base_path}/{fn_name}/{key}.bin"

    def _meta_path(self, fn_name: str, key: str) -> str:
        return f"{self._base_path}/{fn_name}/{key}.meta.json"

    async def get(self, fn_name: str, key: str) -> tuple[bool, bytes | None]:
        """Retrieve cached bytes.

        Args:
            fn_name: The function/activity name (used as namespace).
            key: The cache key.

        Returns:
            A tuple of ``(hit, raw_bytes)``. If ``hit`` is False,
            ``raw_bytes`` is None.
        """
        meta_path = self._meta_path(fn_name, key)

        if not self._fs.exists(meta_path):
            return False, None

        meta = json.loads(self._fs.cat_file(meta_path))
        expires_at = meta.get("expires_at")
        if expires_at is not None:
            if datetime.fromisoformat(expires_at) < datetime.now(timezone.utc):
                self._delete_entry(fn_name, key)
                return False, None

        value_path = self._value_path(fn_name, key)
        if not self._fs.exists(value_path):
            return False, None

        data: bytes = self._fs.cat_file(value_path)
        return True, data

    async def set(
        self,
        fn_name: str,
        key: str,
        data: bytes,
        ttl: timedelta | None = None,
    ) -> None:
        """Store raw bytes in the cache.

        Args:
            fn_name: The function/activity name (used as namespace).
            key: The cache key.
            data: The raw bytes to store.
            ttl: Optional time-to-live. If None, the entry never expires.
        """
        meta: dict[str, str] = {}
        if ttl is not None:
            expires_at = datetime.now(timezone.utc) + ttl
            meta["expires_at"] = expires_at.isoformat()

        value_path = self._value_path(fn_name, key)
        meta_path = self._meta_path(fn_name, key)

        self._fs.pipe_file(value_path, data)
        self._fs.pipe_file(meta_path, json.dumps(meta).encode())

    async def delete(self, fn_name: str, key: str) -> None:
        """Delete a cache entry.

        Args:
            fn_name: The function/activity name (used as namespace).
            key: The cache key.
        """
        self._delete_entry(fn_name, key)

    def _delete_entry(self, fn_name: str, key: str) -> None:
        for path in [
            self._value_path(fn_name, key),
            self._meta_path(fn_name, key),
        ]:
            if self._fs.exists(path):
                self._fs.rm(path)

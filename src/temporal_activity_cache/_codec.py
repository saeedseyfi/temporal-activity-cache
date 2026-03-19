"""Value encoding/decoding using Temporal's PayloadConverter.

Uses the activity's configured PayloadConverter when running inside a
Temporal activity. Falls back to the default converter otherwise.
"""

from __future__ import annotations

from typing import Any

from temporalio.api.common.v1 import Payload
from temporalio.converter import PayloadConverter


def _get_converter() -> PayloadConverter:
    """Get the PayloadConverter for the current context.

    Inside a Temporal activity, returns the worker's configured converter
    (with activity serialization context). Outside an activity, returns
    the default converter.
    """
    try:
        import temporalio.activity

        return temporalio.activity.payload_converter()
    except RuntimeError:
        # Not inside an activity context
        return PayloadConverter.default


def encode_value(value: Any) -> bytes:
    """Serialize a value to bytes using the Temporal PayloadConverter.

    Args:
        value: The value to serialize. Must be supported by the
            configured PayloadConverter (JSON-serializable types,
            dataclasses, Pydantic models, etc.).

    Returns:
        Raw bytes (serialized Payload protobuf).
    """
    converter = _get_converter()
    payloads = converter.to_payloads([value])
    return payloads[0].SerializeToString()


def decode_value(data: bytes, type_hint: type | None = None) -> Any:
    """Deserialize bytes back to a value using the Temporal PayloadConverter.

    Args:
        data: Raw bytes from :func:`encode_value`.
        type_hint: The expected return type. Required for reconstructing
            dataclasses and Pydantic models from JSON. If None, JSON
            values are returned as plain dicts/lists.

    Returns:
        The deserialized value.
    """
    converter = _get_converter()
    payload = Payload()
    payload.ParseFromString(data)
    type_hints = [type_hint] if type_hint is not None else None
    results = converter.from_payloads([payload], type_hints)
    return results[0]

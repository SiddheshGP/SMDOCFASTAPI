from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
import json
import os

app = FastAPI(title="Generic Metadata API", version="1.0")

CONFIG_PATH = "config"

# ---------- MODELS ----------

class IngestRequest(BaseModel):
    consumer_id: str
    metadata: Dict[str, Any]


# ---------- UTILS ----------

def load_consumer_config(consumer_id: str) -> dict:
    """Load metadata schema and mapping for the given consumer"""
    config_file = os.path.join(CONFIG_PATH, f"{consumer_id}.json")
    if not os.path.exists(config_file):
        raise HTTPException(status_code=404, detail=f"No config found for consumer: {consumer_id}")
    
    with open(config_file, "r") as f:
        return json.load(f)

def validate_field_type(field_name: str, value: Any, expected_type: Any):
    """
    Validate data type of a single field.
    Supports:
      - string, number, boolean, array, object, array_of_object
      - nested object schemas via { "type": "object", "schema": {...} }
    """
    # If the expected_type is a dict, it might be a nested schema definition
    if isinstance(expected_type, dict):
        type_def = expected_type.get("type")
        if type_def == "object":
            if not isinstance(value, dict):
                return f"expected object for '{field_name}', got {type(value).__name__}"
            nested_schema = expected_type.get("schema", {})
            for nested_field, nested_type in nested_schema.items():
                if nested_field not in value:
                    return f"missing nested field '{nested_field}' in '{field_name}'"
                error = validate_field_type(f"{field_name}.{nested_field}", value[nested_field], nested_type)
                if error:
                    return error
        else:
            return f"unsupported nested type structure in '{field_name}'"
        return None
    
    
    """
    Validate data type of a single field, supporting:
    string, number, boolean, array, object, array_of_object
    """
    # Map of expected types to Python type checks
    if expected_type == "string":
        if not isinstance(value, str):
            return f"expected string, got {type(value).__name__}"

    elif expected_type == "number":
        if not isinstance(value, (int, float)):
            return f"expected number, got {type(value).__name__}"

    elif expected_type == "boolean":
        if not isinstance(value, bool):
            return f"expected boolean, got {type(value).__name__}"

    elif expected_type == "array":
        if not isinstance(value, list):
            return f"expected array, got {type(value).__name__}"

    elif expected_type == "object":
        if not isinstance(value, dict):
            return f"expected object, got {type(value).__name__}"

    elif expected_type == "array_of_object":
        if not isinstance(value, list):
            return f"expected array of objects, got {type(value).__name__}"
        if not all(isinstance(item, dict) for item in value):
            return f"expected array of objects, but one or more elements are not objects"

    else:
        return f"unsupported data type '{expected_type}' in config"

    return None  # means valid




def validate_metadata(metadata: dict, required_fields: list, field_types: dict):
    """Ensure required fields exist"""
     # 1️⃣ Check for missing fields
    missing = [f for f in required_fields if f not in metadata]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")
        # 2️⃣ Check data type for each field
    for field, expected_type in field_types.items():
        if field in metadata:
            error_msg = validate_field_type(field, metadata[field], expected_type)
            if error_msg:
                raise HTTPException(
                    status_code=400,
                    detail=f"Incorrect data type for field '{field}': {error_msg}"
                    #detail=f"Incorrect data type for field '{error_msg.split('for ')[-1].split(',')[0].strip(\"'\") if 'for ' in error_msg else field}': {error_msg}"
                )


def normalize_metadata(metadata: dict, mappings: dict) -> dict:
    """Map consumer fields to internal representation"""
    normalized = {}
    for consumer_key, internal_key in mappings.items():
        if consumer_key in metadata:
            normalized[internal_key] = metadata[consumer_key]
    return normalized


# ---------- ROUTE ----------

@app.post("/ingest")
async def ingest(request: IngestRequest):
    """Generic ingestion endpoint"""
    try:
        consumer_id = request.consumer_id
        metadata = request.metadata

        # Load consumer-specific config
        config = load_consumer_config(consumer_id)

        # Validate incoming metadata
        validate_metadata(
            metadata,
            config.get("required_fields", []),
            config.get("field_types", {})
        )


        # Normalize metadata to internal structure
        normalized = normalize_metadata(metadata, config.get("mappings", {}))

        # Example business logic: log or push to Pub/Sub
        print(f"✅ Normalized message for {consumer_id}: {normalized}")

        return {
            "message": f"Data from {consumer_id} processed successfully",
            "normalized_payload": normalized
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


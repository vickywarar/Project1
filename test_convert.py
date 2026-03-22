import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock

from app.file_to_file_service import app

client = TestClient(app)


# =========================
# HEALTH CHECK
# =========================
def test_health():
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


# =========================
# MOCK DATA
# =========================
def sample_request():
    return {
        "source": {
            "path": "files/input.csv",
            "type": "csv",
            "has_header": True
        },
        "target": {
            "path": "files/output.json",
            "type": "json"
        },
        "source_avro_schema": {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "id", "type": "string"}]
        },
        "target_avro_schema": {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "id", "type": "string"}]
        }
    }


# =========================
# SUCCESS CASE
# =========================
@patch("app.file_to_file_service.FileReader.read_csv", new_callable=AsyncMock)
@patch("app.file_to_file_service.ETLPipeline.process_batch", new_callable=AsyncMock)
@patch("app.file_to_file_service.FileWriter.write_json", new_callable=AsyncMock)
@patch("app.file_to_file_service.publish_audit_event", new_callable=AsyncMock)
def test_convert_success(mock_audit, mock_write, mock_process, mock_read):
    mock_read.return_value = [{"id": "1"}, {"id": "2"}]
    mock_process.return_value = [{"id": "1"}, {"id": "2"}]
    mock_write.return_value = "files/output.json"

    res = client.post("/convert", json=sample_request())

    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "OK"
    assert data["records_converted"] == 2


# =========================
# EMPTY RECORDS
# =========================
@patch("app.file_to_file_service.FileReader.read_csv", new_callable=AsyncMock)
def test_convert_empty_records(mock_read):
    mock_read.return_value = []

    res = client.post("/convert", json=sample_request())

    assert res.status_code == 400
    assert "No records found" in res.text


# =========================
# UNSUPPORTED FILE TYPE
# =========================
def test_invalid_source_type():
    payload = sample_request()
    payload["source"]["type"] = "invalid"

    res = client.post("/convert", json=payload)

    assert res.status_code == 400
    assert "Unsupported source file type" in res.text


# =========================
# EXCEPTION FLOW
# =========================
@patch("app.file_to_file_service.FileReader.read_csv", new_callable=AsyncMock)
def test_convert_exception(mock_read):
    mock_read.side_effect = Exception("Test error")

    res = client.post("/convert", json=sample_request())

    assert res.status_code == 500
    assert "Conversion failed" in res.text


# =========================
# TARGET WRITE ERROR
# =========================
@patch("app.file_to_file_service.FileReader.read_csv", new_callable=AsyncMock)
@patch("app.file_to_file_service.ETLPipeline.process_batch", new_callable=AsyncMock)
@patch("app.file_to_file_service.FileWriter.write_json", new_callable=AsyncMock)
def test_write_failure(mock_write, mock_process, mock_read):
    mock_read.return_value = [{"id": "1"}]
    mock_process.return_value = [{"id": "1"}]
    mock_write.side_effect = Exception("Write failed")

    res = client.post("/convert", json=sample_request())

    assert res.status_code == 500
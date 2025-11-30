from unittest.mock import MagicMock
from c_nov2025 import pipeline

def test_main(monkeypatch):
    mock_load = MagicMock()
    mock_refine = MagicMock()
    monkeypatch.setattr(pipeline, "load_raw_data", mock_load)
    monkeypatch.setattr(pipeline, "refine_raw_data", mock_refine)

    pipeline.main()

    mock_load.assert_called()
    mock_refine.assert_called()
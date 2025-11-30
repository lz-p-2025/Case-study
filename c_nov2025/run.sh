#!/bin/bash
# Ensure that you are in the directory /Interview/c_nov2025

# install requirements
pip install -r requirements.txt

# Spark runs on JAVA 8/11/17. See docs: https://spark.apache.org/docs/latest/

# Run the pipeline - it may be python instead of python3 depending on how your
# python was installed
python3 pipeline.py


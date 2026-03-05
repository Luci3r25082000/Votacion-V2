"""Compat shim.

- Flask app: flask_app.py
- Streamlit backup: streamlit_app.py

This file exists so platforms/tools that look for app.py can still find a Flask `app`.
"""

from flask_app import create_app

app = create_app()

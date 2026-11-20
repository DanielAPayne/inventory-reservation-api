FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM base AS api
COPY inventory ./inventory
USER 10001:10001
CMD ["python", "-m", "uvicorn", "inventory.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--backlog", "2048", "--no-access-log"]

FROM base AS verifier
COPY tests/requirements.txt /tmp/test-requirements.txt
RUN pip install --no-cache-dir -r /tmp/test-requirements.txt
COPY inventory ./inventory
COPY tests ./tests
USER 10001:10001
CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-v"]

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS uv_builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
# Disable python downloads to use the one from the base image
ENV UV_PYTHON_DOWNLOADS=0
RUN apt-get update && apt-get install -y git

WORKDIR /app

COPY tools/src /app/src
COPY README.md /app/README.md
COPY LICENCE.md /app/LICENCE.md
COPY tools/pyproject.toml /app/pyproject.toml
COPY tools/uv.lock /app/uv.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev


FROM python:3.12.11-slim-trixie AS production
COPY --from=uv_builder --chown=app:app /app /app

# Add ps 
RUN apt-get update && apt-get install -y procps

# # Configure PATH to use the virtual environment's binaries
ENV PATH="/app/.venv/bin:$PATH"
CMD ["bin/bash"]
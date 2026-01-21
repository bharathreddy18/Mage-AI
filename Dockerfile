FROM mageai/mageai:latest

ARG USER_CODE_PATH=/home/src
WORKDIR ${USER_CODE_PATH}

# 1. Create the virtual environment in /opt/venv
RUN python3 -m venv /opt/venv

# 2. Install requirements directly using the venv's pip
# This avoids any symlink or PATH issues during build
COPY requirements.txt .
RUN /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# 3. Set standard environment variables
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="${PYTHONPATH}:${USER_CODE_PATH}/mage"
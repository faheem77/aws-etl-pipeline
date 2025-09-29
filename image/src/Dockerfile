FROM public.ecr.aws/lambda/python:3.11

# Install build tools & libraries needed for scientific Python packages
RUN yum install -y \
    gcc g++ make cmake \
    libpq-devel \
    libffi-devel \
    openssl-devel \
    git \
    && yum clean all

# Copy requirements and install Python deps
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

# Copy source code
COPY . ${LAMBDA_TASK_ROOT}/

# Command for Lambda
CMD ["main.lambda_handler"]
FROM public.ecr.aws/lambda/python:3.11

# Install build tools
RUN yum install -y gcc g++ make cmake libpq-devel libffi-devel openssl-devel git \
    && yum clean all

# Copy everything at once
COPY . ${LAMBDA_TASK_ROOT}/

# Install Python deps
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Lambda handler
CMD ["main.lambda_handler"]
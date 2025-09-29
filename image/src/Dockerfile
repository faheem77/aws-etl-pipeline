FROM public.ecr.aws/lambda/python:3.11

# Copy requirements and install Python deps
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

# Copy source code
COPY . ${LAMBDA_TASK_ROOT}/

# Command for Lambda
CMD ["main.lambda_handler"]
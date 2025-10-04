<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
</head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; margin: 20px;">

  <h1>🧩 AWS Lambda ETL Pipeline — S3 → Snowflake & Elasticsearch</h1>

  <h2>📘 Overview</h2>
  <p>
    This project implements a <strong>serverless ETL (Extract, Transform, Load)</strong> pipeline using
    <strong>AWS Lambda</strong>, <strong>S3</strong>, <strong>Snowflake</strong>, and <strong>Elasticsearch</strong>.
  </p>
  <p>
    Whenever a new CSV file is uploaded to a specific <strong>S3 bucket</strong>, an <strong>AWS Lambda function</strong> is triggered automatically.
    The Lambda:
  </p>
  <ol>
    <li>Reads the CSV file from S3</li>
    <li>Performs data validation, cleaning, and transformation using <strong>pandas</strong> and <strong>numpy</strong></li>
    <li>Loads the processed data into <strong>Snowflake</strong> for analytics</li>
    <li>Indexes the same data into <strong>Elasticsearch</strong> for search and visualization</li>
  </ol>
  <p>This solution provides a low-maintenance, scalable, and cost-effective ETL flow.</p>

  <hr />

  <h2>🏗️ Architecture</h2>
  <pre style="background-color:#f4f4f4; padding:10px; border-radius:5px;">
         +-------------+
         |   CSV File  |
         | (Uploaded)  |
         +------+------+
                |
                v
         +------+------+
         |   S3 Bucket |
         +------+------+
                |
      (S3 Event Trigger)
                |
                v
         +------+------+
         | AWS Lambda  |
         | (Python 3.11|
         |  + pandas)  |
         +------+------+
        /        \
       v          v
+------------+  +----------------+
| Snowflake  |  | Elasticsearch  |
|  Data Lake |  |   Index Store  |
+------------+  +----------------+
  </pre>

  <h2>⚙️ Tech Stack</h2>
  <ul>
    <li><strong>AWS S3</strong> — Data source and event trigger</li>
    <li><strong>AWS Lambda</strong> — Serverless compute engine</li>
    <li><strong>Snowflake</strong> — Cloud data warehouse for analytics</li>
    <li><strong>Elasticsearch</strong> — Full-text search and indexing</li>
    <li><strong>Docker</strong> — Lambda container packaging</li>
    <li><strong>Python 3.11</strong> — ETL logic (pandas, numpy, pyarrow, snowflake-connector-python, elasticsearch-py)</li>
  </ul>

  <hr />

  <h2>🧰 Environment Variables</h2>
  <p>Set the following environment variables in AWS Lambda configuration:</p>
  <table border="1" cellspacing="0" cellpadding="6">
    <tr><th>Variable Name</th><th>Description</th></tr>
    <tr><td>SNOWFLAKE_USER</td><td>Snowflake username</td></tr>
    <tr><td>SNOWFLAKE_PASSWORD</td><td>Snowflake password</td></tr>
    <tr><td>SNOWFLAKE_ACCOUNT</td><td>Snowflake account identifier</td></tr>
    <tr><td>SNOWFLAKE_WAREHOUSE</td><td>Snowflake warehouse name</td></tr>
    <tr><td>SNOWFLAKE_DATABASE</td><td>Target database</td></tr>
    <tr><td>SNOWFLAKE_SCHEMA</td><td>Target schema</td></tr>
    <tr><td>ELASTICSEARCH_HOST</td><td>Elasticsearch host URL</td></tr>
    <tr><td>ELASTICSEARCH_USER</td><td>Elasticsearch username</td></tr>
    <tr><td>ELASTICSEARCH_PASS</td><td>Elasticsearch password</td></tr>
  </table>

  <hr />

  <h2>🚀 Deployment Steps</h2>

  <h3>1. Build the Docker Image</h3>
  <pre><code>docker buildx build \
  --platform linux/amd64 \
  -t &lt;aws_account_id&gt;.dkr.ecr.&lt;region&gt;.amazonaws.com/data_etl_project:latest .</code></pre>

  <h3>2. Push Image to ECR</h3>
  <pre><code>aws ecr get-login-password --region &lt;region&gt; | docker login --username AWS --password-stdin &lt;aws_account_id&gt;.dkr.ecr.&lt;region&gt;.amazonaws.com
docker push &lt;aws_account_id&gt;.dkr.ecr.&lt;region&gt;.amazonaws.com/etl_project:latest</code></pre>

  <h3>3. Update AWS Lambda</h3>
  <pre><code>aws lambda update-function-code \
  --function-name etl_project_lambda \
  --image-uri &lt;aws_account_id&gt;.dkr.ecr.&lt;region&gt;.amazonaws.com/etl_project:latest \
  --region &lt;region&gt;</code></pre>

  <hr />

  <h2>🧩 Data Flow Summary</h2>
  <ol>
    <li>User uploads a CSV file into S3 bucket.</li>
    <li>S3 event triggers the Lambda function.</li>
    <li>Lambda reads and validates the CSV.</li>
    <li>Data is transformed using pandas.</li>
    <li>Cleaned data is written to:
      <ul>
        <li><strong>Snowflake table</strong> (via snowflake.connector)</li>
        <li><strong>Elasticsearch index</strong> (via elasticsearch client)</li>
      </ul>
    </li>
  </ol>

  <hr />

  <h2>🧪 Local Testing</h2>
  <pre><code>docker run -p 9000:8080 \
  &lt;aws_account_id&gt;.dkr.ecr.&lt;region&gt;.amazonaws.com/etl_project:latest

# Then invoke using:
aws lambda invoke \
  --endpoint-url http://localhost:9000 \
  --no-sign-request \
  --function-name etl_project_lambda \
  response.json</code></pre>

  <hr />

  <h2>🩺 Troubleshooting</h2>
  <table border="1" cellspacing="0" cellpadding="6">
    <tr><th>Issue</th><th>Possible Cause</th><th>Fix</th></tr>
    <tr><td>Runtime.OutOfMemory</td><td>Large pandas DataFrame</td><td>Use smaller chunks or increase Lambda memory</td></tr>
    <tr><td>InvalidParameterValueException</td><td>Unsupported image manifest</td><td>Rebuild using <code>--platform linux/amd64</code></td></tr>
    <tr><td>Snowflake load fails</td><td>Incorrect credentials or permissions</td><td>Verify Snowflake connection details</td></tr>
    <tr><td>Elasticsearch indexing fails</td><td>Wrong index name or mapping</td><td>Check index name and schema</td></tr>
  </table>

  <hr />

  <h2>📄 License</h2>
  <p>
    This project is licensed under the <strong>MIT License</strong> — feel free to use and modify.
  </p>

</body>
</html>
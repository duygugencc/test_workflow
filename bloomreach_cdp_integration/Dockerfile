FROM prefecthq/prefect:2.14-python3.11

ARG _ENV
ARG ACCESS_TOKEN
ENV env=$_ENV

# Install system dependencies and Google Cloud SDK
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-456.0.0-linux-x86_64.tar.gz && \
    tar -xf google-cloud-sdk-456.0.0-linux-x86_64.tar.gz && \
    ./google-cloud-sdk/install.sh --quiet && \
    mv google-cloud-sdk /usr/local/gcloud && \
    rm google-cloud-sdk-456.0.0-linux-x86_64.tar.gz

ENV PATH="/usr/local/gcloud/bin:$PATH"

COPY requirements.txt /opt/prefect/bloomreach_cdp_integration/requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /opt/prefect/bloomreach_cdp_integration/requirements.txt && \
    pip install --no-cache-dir bq-ingestor -c /opt/prefect/bloomreach_cdp_integration/requirements.txt --extra-index-url "https://oauth2accesstoken:${ACCESS_TOKEN}@europe-north1-python.pkg.dev/data-domain-data-warehouse/python-packages/simple"

COPY . /opt/prefect/bloomreach_cdp_integration
WORKDIR /opt/prefect/bloomreach_cdp_integration

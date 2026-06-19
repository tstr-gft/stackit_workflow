from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


DEFAULT_IMAGE = "docker.getcollate.io/openmetadata/ingestion:1.12.9"
DEFAULT_COMMAND = "metadata ingest -c /opt/airflow/dags/config/dremio_pipeline.yml"


with DAG(
    dag_id="openmetadata_dremio_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["openmetadata", "dremio", "stackit"],
) as dag:
    run_ingestion = KubernetesPodOperator(
        task_id="run_openmetadata_dremio_ingestion",
        name="openmetadata-dremio-ingestion",
        image=Variable.get("openmetadata_ingestion_image", default_var=DEFAULT_IMAGE),
        cmds=["bash", "-lc"],
        arguments=[Variable.get("openmetadata_ingestion_command", default_var=DEFAULT_COMMAND)],
        env_vars={
            "OPENMETADATA_SERVER_URL": Variable.get(
                "openmetadata_server_url",
                default_var="http://127.0.0.1:8585/api",
            ),
            "OPENMETADATA_JWT_TOKEN": Variable.get("openmetadata_jwt_token", default_var=""),
            "DREMIO_HOST": Variable.get(
                "dremio_host",
                default_var="flight.675fb8aa-0107-4e29-a200-1472addf9dac.dremio.eu01.onstackit.cloud",
            ),
            "DREMIO_PORT": Variable.get("dremio_port", default_var="443"),
            "DREMIO_USERNAME": Variable.get("dremio_username", default_var="admin"),
            "DREMIO_PASSWORD": Variable.get("dremio_password", default_var=""),
            "DREMIO_DATABASE": Variable.get("dremio_database", default_var="coedata"),
        },
        get_logs=True,
        is_delete_operator_pod=True,
    )

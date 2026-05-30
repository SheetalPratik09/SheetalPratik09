# Data Pipeline (Airflow) Exercises

🔭 I’m currently working on building and validating end-to-end data pipelines using Apache Airflow, AWS Redshift, S3, and custom operators.

## About this repository

This repo contains Udacity Data Engineering course exercises and project work for Airflow-based pipelines.

It includes:
- lesson exercises for Airflow, AWS, and data quality
- starter and solution code for coursework
- custom operators for staging, loading, and data quality checks
- a full project DAG with staging, fact/dimension loading, and quality checks

## How to use

1. Copy `udacity/common` and any `.airflowignore` files into your Airflow DAG directory.
2. Copy `custom_operators` or the custom plugin code into your Airflow plugins directory.
3. Update connection scripts and environment variables to match your AWS credentials and Redshift setup.
4. Start Airflow and verify that DAGs are loaded correctly.

## Important files

- `docker-compose.yaml` — Airflow stack configuration
- `project/starter/final_project.py` — primary project DAG definition
- `project/starter/final_project_operators/` — custom operator implementations
- `set_connections.sh` — sample connection setup script
- `requirements.txt` — Airflow and provider dependencies

## Notes

- The `set_connections.sh` script is only a template and should be updated with your actual AWS and Redshift credentials through secure environment variables or Airflow connections.
- Avoid committing real credentials into Git.
- Use the DAG dependency chains and task definitions from the project starter files for correct pipeline execution.

## Status

✅ Local updates are committed and the repository is ready for development.

## Next steps

- Validate the project DAG and custom operator code in your Airflow environment.
- Run the project DAG and confirm that staging, loading, and quality checks complete successfully.
- Keep secrets out of source control by using Airflow connections, environment variables, or secret management.

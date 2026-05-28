# Astro Employee Project

This is an Apache Airflow project built with Astronomer.

## Project Structure

```
astro-employee-project/
├── dags/                      # DAG files
│   └── employee_pipeline.py   # Main employee data pipeline
├── plugins/                   # Custom plugins and operators
├── include/                   # Shared libraries and utilities
├── tests/                     # Unit tests for DAGs
├── .astro/                    # Astronomer configuration
├── requirements.txt           # Python dependencies
├── packages.txt              # System-level dependencies
├── Dockerfile                # Docker configuration
├── airflow_settings.yaml     # Airflow settings
└── README.md                 # This file
```

## Getting Started

### Prerequisites
- Docker
- Astronomer CLI

### Setup

1. Install Astronomer CLI (if not already installed):
```bash
brew install astronomer-cli
```

2. Start the local Airflow environment:
```bash
astro dev start
```

3. Access the Airflow UI:
- URL: http://localhost:8080
- Default credentials: admin / admin

### Project Configuration

- **Requirements**: Edit `requirements.txt` to add Python dependencies
- **System Packages**: Edit `packages.txt` to add system-level dependencies
- **Settings**: Configure `airflow_settings.yaml` for Airflow settings

## DAGs

### employee_pipeline.py
Main pipeline for processing employee data.

## Development

To add a new DAG:
1. Create a new Python file in the `dags/` directory
2. Define your DAG with proper documentation
3. Restart the Airflow environment or let it auto-reload

## Testing

Add tests in the `tests/` directory and run:
```bash
pytest tests/
```

## Deployment

To deploy this project to Astronomer Cloud:
```bash
astro deploy
```

## Documentation

For more information, visit:
- [Astronomer Documentation](https://docs.astronomer.io)
- [Apache Airflow Documentation](https://airflow.apache.org/)

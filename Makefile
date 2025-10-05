# Makefile - commandes courantes pour le projet WeatherStack ETL

.DEFAULT_GOAL := help

# Couleurs
GREEN=\033[32m
BLUE=\033[34m
YELLOW=\033[33m
RESET=\033[0m

help: ## Affiche cette aide
	@echo "$(BLUE)Commandes disponibles :$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?##' Makefile | sed -E 's/:.*?##/: /' | sort | awk '{printf "  $(GREEN)%-25s$(RESET) %s\n", $$1, substr($$0, index($$0,$$2))}'

up: ## Démarre l'infrastructure (build + detached)
	docker compose up -d --build

up-no-build: ## Démarre sans reconstruire
	docker compose up -d

stop: ## Stoppe les conteneurs sans supprimer
	docker compose stop

down: ## Stoppe et supprime les conteneurs
	docker compose down

logs-web: ## Logs webserver
	docker compose logs -f airflow-webserver

logs-scheduler: ## Logs scheduler
	docker compose logs -f airflow-scheduler

ps: ## Affiche l'état des services
	docker compose ps

bash-dbt: ## Ouvre un shell dans le workspace dbt-cli
	docker compose exec dbt-cli bash

bash-web: ## Shell dans webserver Airflow
	docker compose exec airflow-webserver bash

bash-scheduler: ## Shell dans scheduler Airflow
	docker compose exec airflow-scheduler bash

dbt-deps: ## Installe/Met à jour les packages dbt (dbt-utils, etc.)
	docker compose exec dbt-cli dbt deps

dbt-staging: ## Exécute uniquement les modèles staging
	docker compose exec dbt-cli dbt run --select staging

dbt-marts: ## Exécute uniquement les modèles marts
	docker compose exec dbt-cli dbt run --select marts

dbt-build: ## Build complet (run + tests)
	docker compose exec dbt-cli dbt build

dbt-test: ## Tests dbt (tous)
	docker compose exec dbt-cli dbt test

dbt-docs: ## Génère la documentation dbt
	docker compose exec dbt-cli dbt docs generate

dbt-docs-serve: ## Sert la documentation (bloquant)
	docker compose exec dbt-cli dbt docs serve --port 8081

fresh-run: ## ETL complet (staging + marts + tests) via Airflow (déclenchement manuel du DAG full pipeline)
	@echo "Déclencher dans l'UI ou utiliser l'API Airflow si configurée."

clean-target: ## Nettoie répertoires compilés dbt
	rm -rf dbt/target dbt/dbt_packages

rebuild: clean-target ## Reconstruit l'infra from scratch
	docker compose down -v
	docker compose up -d --build

.PHONY: help up up-no-build stop down logs-web logs-scheduler ps bash-dbt bash-web bash-scheduler dbt-deps dbt-staging dbt-marts dbt-build dbt-test dbt-docs dbt-docs-serve fresh-run clean-target rebuild

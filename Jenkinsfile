pipeline {
    agent any

    environment {
        PYTHON = 'python3'
        RUN_INTEGRATION_TESTS = 'true'
        RUN_E2E_TESTS = 'true'
        KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
        POSTGRES_HOST = 'postgres'
        POSTGRES_PORT = '5432'
        API_URL = 'http://sales-api:8000'
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Environment') {
            steps {
                sh 'python3 --version'
                sh 'docker --version'
            }
        }

        stage('Install') {
            steps {
                sh 'python3 -m pip install --user --break-system-packages -r requirements.txt'
            }
        }

        stage('Unit Tests') {
            steps {
                sh '''
                    python3 -m pytest tests/unit \
                      --cov=app \
                      --cov-report=xml:coverage.xml \
                      --cov-report=term-missing \
                      --junitxml=test-results-unit.xml
                '''
            }
        }

        stage('Integration Tests') {
            steps {
                sh '''
                    python3 -m pytest tests/integration \
                      --junitxml=test-results-integration.xml
                '''
            }
        }

        stage('Build') {
            steps {
                sh 'docker compose build sales-api spark-streaming'
            }
        }

        stage('E2E Tests') {
            steps {
                sh '''
                    python3 -m pytest tests/e2e \
                      --junitxml=test-results-e2e.xml
                '''
            }
        }

        stage('SonarQube') {
            steps {
                script {
                    def scannerHome = tool 'SonarScanner'
                    withSonarQubeEnv('SonarQube') {
                        sh "${scannerHome}/bin/sonar-scanner"
                    }
                }
            }
        }

        stage('Quality Gate') {
            steps {
                timeout(time: 5, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }
    }

    post {
        always {
            junit allowEmptyResults: true, testResults: 'test-results-*.xml'
            archiveArtifacts allowEmptyArchive: true, artifacts: 'coverage.xml'
        }
    }
}

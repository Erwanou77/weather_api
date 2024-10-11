## Prérequis

Avant de démarrer, assurez-vous d'avoir installé les outils suivants sur votre machine :

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Git](https://git-scm.com/)

## Installation

### Étape 1 : Cloner le dépôt

Clonez le projet à l'aide de la commande suivante :

```bash
git clone https://github.com/votre-utilisateur/votre-projet.git
```

### Étape 2 : Configurer les variables d'environnement

Avant de démarrer l'application, vous devez configurer certaines variables d'environnement. Créez un fichier .env à la racine du projet en vous basant sur l'exemple .env.example.

**Vous devez créer un .env dans le répertoire python et un à la racine**

### Étape 3 : Lancer le projet avec Docker Compose

Utilisez Docker Compose pour démarrer l'application et les services associés (MySQL, etc.).

```bash
docker compose up -d
```

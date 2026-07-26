# Integration deployment SDLC

`develop` is the integration branch and `main` is production. Changes are
deployed only for active integration roots containing both `prefect.yaml` and
`cloudbuild.yaml`; nested integrations are resolved to their nearest matching
root and `archive/` is never deployed.

## Lifecycle

1. Open a pull request into `develop`. **PR develop gate** detects changed
   integrations and uses Cloud Build to build each Docker image in isolation.
   It does not push the image, create a trigger, or call `prefect deploy`.
   Configure the job named `PR develop gate` as a required branch check.
2. Run the changed flow locally for functional testing. Image validation proves
   that the container can build, but does not prove that the flow behaves
   correctly with real data and credentials.
3. Merge into `develop`. **Merge develop confirm** creates a missing dev trigger,
   deploys with the `dev` substitution, updates `<integration>_dev`, and waits
   for Cloud Build. A failure opens a GitHub issue.
4. Open a PR from `develop` into `main`. On merge, **Merge main prod deploy**
   creates missing prod triggers, rebuilds with the `prod` substitution,
   updates `<integration>_prod`, and waits for Cloud Build success. A failure
   opens a GitHub issue.

## Repository setup

Create the `develop` branch, protect `develop` with the required check above,
and protect `main` so changes arrive through reviewed PRs from `develop`.
Add the repository secret `GCP_CI_SA_KEY`. Its service account needs permission
to submit/inspect builds, inspect/import/run Cloud Build triggers, plus
`iam.serviceAccounts.actAs` for the configured
`de-sgds-integration@sambla-data-staging-compliance.iam.gserviceaccount.com`
build service account.

Because a PR can change its Dockerfile, run PR validation with a dedicated,
least-privilege Cloud Build service account. It should have read-only access to
the private Python/Artifact Registry packages needed during the build and no
Prefect secret access or production mutation permissions.

Trigger naming and exceptional service accounts/substitution names live in
[`integration-overrides.json`](integration-overrides.json). The default names
are `<integration-directory-with-dashes>-dev` and `...-prod`. Existing triggers
are reused as-is; this automation deliberately does not overwrite them.

The PR Cloud Build proves only that the Docker image builds. After merge, a dev
Cloud Build success proves that Prefect accepted the deployment. Neither proves
that an actual flow run succeeded; run locally before merge and execute the dev
deployment for integration/UAT coverage before promoting to `main`.

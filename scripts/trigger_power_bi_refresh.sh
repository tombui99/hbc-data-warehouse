#!/usr/bin/env bash

set -euo pipefail

required_variables=(
  PBI_TENANT_ID
  PBI_CLIENT_ID
  PBI_CLIENT_SECRET
  PBI_WORKSPACE_ID
  PBI_DATASET_ID
)

for variable_name in "${required_variables[@]}"; do
  if [[ -z "${!variable_name:-}" ]]; then
    echo "Missing required GitHub Actions secret: ${variable_name}" >&2
    exit 1
  fi
done

uuid_pattern='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
for id_variable in PBI_WORKSPACE_ID PBI_DATASET_ID; do
  if [[ ! "${!id_variable}" =~ ${uuid_pattern} ]]; then
    echo "${id_variable} must be a plain UUID with no quotes, URL, or whitespace." >&2
    exit 1
  fi
done

response_file=$(mktemp)
trap 'rm -f "${response_file}"' EXIT

http_status=$(curl --silent --show-error \
  --output "${response_file}" \
  --write-out "%{http_code}" \
  -X POST "https://login.microsoftonline.com/${PBI_TENANT_ID}/oauth2/v2.0/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "client_id=${PBI_CLIENT_ID}" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "scope=https://analysis.windows.net/powerbi/api/.default" \
  --data-urlencode "client_secret=${PBI_CLIENT_SECRET}")

if [[ "${http_status}" != "200" ]]; then
  error_code=$(jq -r '.error // "unknown_error"' "${response_file}")
  error_description=$(jq -r '.error_description // "No error description returned by Azure."' "${response_file}")
  echo "Azure token request failed with HTTP ${http_status}." >&2
  echo "${error_code}: ${error_description}" >&2
  echo "Check PBI_TENANT_ID, PBI_CLIENT_ID, and PBI_CLIENT_SECRET in GitHub Actions secrets." >&2
  exit 1
fi

access_token=$(jq -er '.access_token' "${response_file}")

power_bi_base_url="https://api.powerbi.com/v1.0/myorg"

workspace_status=$(curl --silent --show-error \
  --output "${response_file}" \
  --write-out "%{http_code}" \
  -H "Authorization: Bearer ${access_token}" \
  "${power_bi_base_url}/groups/${PBI_WORKSPACE_ID}")

if [[ "${workspace_status}" != "200" ]]; then
  echo "Power BI workspace preflight failed with HTTP ${workspace_status}." >&2
  if [[ "${workspace_status}" == "404" ]]; then
    echo "The workspace ID is wrong, belongs to another tenant, or the Entra application has not been added to the workspace." >&2
    echo "Ask a workspace admin to add the application as Contributor or Member, then verify PBI_WORKSPACE_ID." >&2
  else
    jq -c '.' "${response_file}" >&2 || true
  fi
  exit 1
fi

echo "Power BI workspace is visible to the service principal."

dataset_status=$(curl --silent --show-error \
  --output "${response_file}" \
  --write-out "%{http_code}" \
  -H "Authorization: Bearer ${access_token}" \
  "${power_bi_base_url}/groups/${PBI_WORKSPACE_ID}/datasets/${PBI_DATASET_ID}")

if [[ "${dataset_status}" != "200" ]]; then
  echo "Power BI semantic-model preflight failed with HTTP ${dataset_status}." >&2
  if [[ "${dataset_status}" == "404" ]]; then
    echo "The workspace is accessible, but PBI_DATASET_ID does not identify a semantic model in that workspace." >&2
    echo "Verify that the semantic-model ID and workspace ID were copied from the same workspace." >&2
  else
    jq -c '.' "${response_file}" >&2 || true
  fi
  exit 1
fi

semantic_model_name=$(jq -r '.name // "(unnamed semantic model)"' "${response_file}")
echo "Power BI semantic model is visible: ${semantic_model_name}"

refresh_status=$(curl --silent --show-error \
  --output "${response_file}" \
  --write-out "%{http_code}" \
  -X POST "${power_bi_base_url}/groups/${PBI_WORKSPACE_ID}/datasets/${PBI_DATASET_ID}/refreshes" \
  -H "Authorization: Bearer ${access_token}" \
  -H "Content-Length: 0")

if [[ "${refresh_status}" != "200" && "${refresh_status}" != "202" ]]; then
  echo "Power BI refresh request failed with HTTP ${refresh_status}." >&2
  jq -c '.' "${response_file}" >&2 || true
  exit 1
fi

echo "Power BI dataset refresh accepted."

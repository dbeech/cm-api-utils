package com.cloudera.ps.cm.api.deployment.transform;

import com.fasterxml.jackson.databind.JsonNode;

public interface DeploymentTransformer {
    public JsonNode transform(JsonNode input);
}

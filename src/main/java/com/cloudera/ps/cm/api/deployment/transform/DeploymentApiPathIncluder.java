package com.cloudera.ps.cm.api.deployment.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map.Entry;

public class DeploymentApiPathIncluder implements DeploymentTransformer {

    private final ObjectMapper mapper = new ObjectMapper();
    private final String apiVersion;
    private final String apiRoot;

    public DeploymentApiPathIncluder(String apiVersion) {
        this.apiVersion = apiVersion;
        this.apiRoot = "/api/" + apiVersion;
    }

    public JsonNode transform(JsonNode input) {
        if (input.getNodeType() == JsonNodeType.OBJECT)
            return addApiPaths((ObjectNode) input, apiRoot);
        return input;
    }

    private JsonNode addApiPaths(ObjectNode input, String apiPath) {
        ObjectNode transformed = mapper.createObjectNode();
        if (!apiRoot.equals(apiPath)) {
            transformed.put("api_path", apiPath);
        }
        Iterator<Entry<String,JsonNode>> fields = input.fields();
        while (fields.hasNext()) {
            Entry<String,JsonNode> field = fields.next();
            String fieldKey = field.getKey();
            if (field.getValue().getNodeType() == JsonNodeType.OBJECT) {
                transformed.set(fieldKey, addApiPaths((ObjectNode) field.getValue().deepCopy(), apiPath + "/" + getApiKey(fieldKey)));
            } else {
                transformed.set(fieldKey, field.getValue());
            }
        }
        return transformed;
    }

    private String getApiKey(String fieldKey) {
        if ("versionInfo".equals(fieldKey))
            return "cm/version";
        if ("managerSettings".equals(fieldKey))
            return "cm/config";
        if ("managementService".equals(fieldKey))
            return "cm/service";
        if ("allHostsConfig".equals(fieldKey)) {
            return "cm/allHosts/config";
        }
        return fieldKey;
    }

}

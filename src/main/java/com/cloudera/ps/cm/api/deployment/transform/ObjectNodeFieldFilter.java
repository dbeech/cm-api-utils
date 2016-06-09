package com.cloudera.ps.cm.api.deployment.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;

public class ObjectNodeFieldFilter implements DeploymentTransformer {

    private final ObjectMapper mapper = new ObjectMapper();

    public JsonNode transform(JsonNode input) {
        if (input.getNodeType() == JsonNodeType.OBJECT)
            return filterObjectNode((ObjectNode) input);
        if (input.getNodeType() == JsonNodeType.ARRAY)
            return filterArrayNode((ArrayNode) input);
        return input;
    }

    private JsonNode filterObjectNode(ObjectNode original) {
        ObjectNode filtered = mapper.createObjectNode();
        Iterator<String> fieldNames = original.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            if ("role_jceks_password".equals(fieldName)) continue;
            filtered.set(fieldName, transform(original.get(fieldName).deepCopy()));
        }
        return filtered;
    }

    private JsonNode filterArrayNode(ArrayNode original) {
        ArrayNode converted = mapper.createArrayNode();
        for (JsonNode element: original) {
            converted.add(transform(element.deepCopy()));
        }
        return converted;
    }

}

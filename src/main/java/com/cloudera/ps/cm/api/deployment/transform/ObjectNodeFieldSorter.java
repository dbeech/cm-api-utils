package com.cloudera.ps.cm.api.deployment.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class ObjectNodeFieldSorter implements DeploymentTransformer {

    private final ObjectMapper mapper = new ObjectMapper();

    public JsonNode transform(JsonNode input) {
        if (input.getNodeType() == JsonNodeType.OBJECT)
            return sortObjectNode((ObjectNode) input);
        if (input.getNodeType() == JsonNodeType.ARRAY)
            return sortArrayNode((ArrayNode) input);
        return input;
    }

    private JsonNode sortObjectNode(ObjectNode original) {
        Iterator<String> fieldNames = original.fieldNames();
        Set<String> sortedFieldNames = new TreeSet<String>();
        while (fieldNames.hasNext()) {
            sortedFieldNames.add(fieldNames.next());
        }
        ObjectNode sorted = mapper.createObjectNode();
        for (String fieldName: sortedFieldNames) {
            sorted.set(fieldName, transform(original.get(fieldName).deepCopy()));
        }
        return sorted;
    }

    private JsonNode sortArrayNode(ArrayNode original) {
        ArrayNode converted = mapper.createArrayNode();
        for (JsonNode element: original) {
            converted.add(transform(element.deepCopy()));
        }
        return converted;
    }

}

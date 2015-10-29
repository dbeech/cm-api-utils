package com.cloudera.ps.cm.api.deployment.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DeploymentReformatter implements DeploymentTransformer {

    private static final Map<String,String> ARRAYS_TO_CONVERT = new HashMap<String,String>(){{
        put("roles", "name");
        put("services", "name");
        put("roleConfigGroups", "name");
        put("clusters", "name");
        put("users", "name");
        put("hosts", "hostId");
    }};

    private final ObjectMapper mapper = new ObjectMapper();
    private Map<String, String> hostIdMap;

    public JsonNode transform(JsonNode input) {
        hostIdMap = buildHostIdMap(input);
        return reformat(input);
    }

    private JsonNode reformat(JsonNode original) {
        switch (original.getNodeType()) {
            case OBJECT:
                return reformatObjectNode((ObjectNode) original);
            case ARRAY:
                return reformatArrayNode((ArrayNode) original);
            default:
                return original;
        }
    }

    private JsonNode reformatObjectNode(ObjectNode original) {
        ObjectNode converted = mapper.createObjectNode();
        Iterator<Map.Entry<String,JsonNode>> fields = original.fields();
        while (fields.hasNext()) {
            Map.Entry<String,JsonNode> next = fields.next();
            String fieldName = next.getKey();
            if ("items".equals(fieldName)) {
                converted.setAll(itemsToObject((ArrayNode) next.getValue()));
            } else if ("parcels".equals(fieldName)) {
                converted.set(fieldName, parcelsToObject((ArrayNode) next.getValue()));
            } else if ("hostRef".equals(fieldName)) {
                // replace host id guid with actual hostname
                converted.put("host", hostIdMap.get(next.getValue().get("hostId").asText()));
            } else if ("clusterRef".equals(fieldName)) {
                converted.set("cluster", next.getValue().get("clusterName"));
            } else if ("roleConfigGroupRef".equals(fieldName)) {
                converted.set("roleConfigGroup", next.getValue().get("roleConfigGroupName"));
            } else if ("serviceRef".equals(fieldName) || "base".equals(fieldName)) {
                // ignore
            } else if (ARRAYS_TO_CONVERT.containsKey(fieldName)) {
                converted.set(fieldName, arrayToObject(ARRAYS_TO_CONVERT.get(fieldName), (ArrayNode) next.getValue()));
            } else {
                converted.set(fieldName, reformat(next.getValue()));
            }
        }
        return converted;
    }

    private JsonNode reformatArrayNode(ArrayNode original) {
        ArrayNode converted = mapper.createArrayNode();
        for (JsonNode element: original) {
            converted.add(reformat(element));
        }
        return converted;
    }


    private JsonNode arrayToObject(String key, ArrayNode array) {
        ObjectNode obj = mapper.createObjectNode();
        for (JsonNode element: array) {
            if (element.getNodeType() == JsonNodeType.OBJECT) {
                ObjectNode elementObject = (ObjectNode) element;
                obj.set(elementObject.remove(key).asText(), elementObject.deepCopy());
            }
        }
        return reformat(obj);
    }

    private JsonNode parcelsToObject(ArrayNode array) {
        ObjectNode obj = mapper.createObjectNode();
        for (JsonNode element: array) {
            if (element.getNodeType() == JsonNodeType.OBJECT) {
                ObjectNode elementObject = (ObjectNode) element;
                String product = elementObject.remove("product").asText();
                String version = elementObject.remove("version").asText();
                obj.set(product + "-" + version, elementObject.deepCopy());
            }
        }
        return reformat(obj);
    }

    private ObjectNode itemsToObject(ArrayNode items) {
        ObjectNode obj = mapper.createObjectNode();
        for (JsonNode item: items) {
            obj.set(item.get("name").asText(), item.get("value"));
        }
        return obj;
    }

    private Map<String,String> buildHostIdMap(JsonNode input) {
        ArrayNode hostsSection = (ArrayNode) input.get("hosts");
        Map<String,String> hostIdMap = new HashMap<String, String>();
        for (JsonNode host: hostsSection) {
            String hostname = host.get("hostname").asText();
            String hostId = host.get("hostId").asText();
            hostIdMap.put(hostId, hostname);
        }
        return hostIdMap;
    }


}

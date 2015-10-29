package com.cloudera.ps.cm.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Iterator;

public class ConfigurationDatabaseLoader {

    private Connection conn;
    private PreparedStatement ps;

    private final ObjectMapper mapper = new ObjectMapper();

    public ConfigurationDatabaseLoader() {
        try {
            Class.forName("org.sqlite.JDBC");
            this.conn = DriverManager.getConnection("jdbc:sqlite:configuration.db");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void createTable() throws SQLException {
        Statement stmt = conn.createStatement();
        String createTableSql = "CREATE TABLE IF NOT EXISTS configuration\n" +
                "(\n" +
                "    product TEXT,\n" +
                "    version TEXT,\n" +
                "    service TEXT,\n" +
                "    role_group TEXT,\n" +
                "    category TEXT,\n" +
                "    api_name TEXT,\n" +
                "    display_name TEXT,\n" +
                "    related_name TEXT,\n" +
                "    description TEXT,\n" +
                "    default_value TEXT,\n" +
                "    units TEXT,\n" +
                "    required INTEGER,\n" +
                "    sensitive INTEGER\n" +
                ");\n";
        stmt.execute(createTableSql);
    }

    public void run() throws IOException, SQLException {
        createTable();
        ps = conn.prepareStatement("INSERT INTO configuration (product, version, service, " +
                "role_group, category, api_name, display_name, related_name, description, default_value, units, " +
                "required, sensitive) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");

        JsonNode config = mapper.readValue(FileUtils.readFileToString(new File("configurationReference.json")), JsonNode.class);
        Iterator keys = config.fieldNames();
        while (keys.hasNext()) {
            String key = keys.next().toString();
            if (key.startsWith("CDH")) {
                insertConfigurationForCDH(config.get(key));
            } else if (key.startsWith("Host")) {
                insertConfigurationForHosts(config.get(key));
            } else if (key.startsWith("Cloudera Manager")) {
                insertConfigurationForClouderaManager(config.get(key));
            } else if (key.startsWith("Cloudera Management")) {
                insertConfigurationForClouderaManagementServices(config.get(key));
            } else {
                System.err.println("Unknown key: " + key);
            }
        }
    }

    public void close() throws SQLException {
        DbUtils.close(ps);
        DbUtils.close(conn);
    }

    private void insertConfigurationForClouderaManager(JsonNode jsonObject) {
        insertCategories(new ConfigurationHeader("Cloudera Manager"), jsonObject.get("categories"));
    }

    private void insertConfigurationForClouderaManagementServices(JsonNode jsonObject) {
        insertRoleGroups(new ConfigurationHeader("Cloudera Management Services"), jsonObject.get("roleGroups"));
    }

    private void insertConfigurationForHosts(JsonNode jsonObject) {
        insertCategories(new ConfigurationHeader("Hosts"), jsonObject.get("categories"));
    }

    private void insertConfigurationForCDH(JsonNode versions) {
        for (JsonNode version : versions) {
            ConfigurationHeader header = new ConfigurationHeader("CDH");
            String versionString = version.get("version").asText().replace("CDH ", "");
            System.out.println("> " + versionString);
            for (JsonNode service : version.get("services")) {
                header.setVersion(versionString);
                header.setServiceType(service.get("serviceType").asText());
                insertRoleGroups(header, service.get("roleGroups"));
            }
        }
    }

    private void insertRoleGroups(ConfigurationHeader header, JsonNode roleGroups) {
        for (JsonNode roleGroup : roleGroups) {
            header.setRoleGroupName(roleGroup.get("name").asText());
            insertCategories(header, roleGroup.get("categories"));
        }
    }

    private void insertCategories(ConfigurationHeader header, JsonNode categories) {
        for (JsonNode category : categories) {
            for (JsonNode roleGroupConfigElement : category.get("configs")) {
                header.setCategoryName(category.get("name").asText());
                insertConfigToDatabase(header, roleGroupConfigElement);
            }
        }
    }

    private void insertConfigToDatabase(ConfigurationHeader configurationHeader, JsonNode config) {
        try {
            ps.setString(1, configurationHeader.getProduct());
            ps.setString(2, configurationHeader.getVersion());
            ps.setString(3, configurationHeader.getServiceType());
            ps.setString(4, configurationHeader.getRoleGroupName());
            ps.setString(5, configurationHeader.getCategoryName());
            ps.setString(6, config.get("apiName").asText());
            ps.setString(7, config.get("displayName").asText());
            ps.setString(8, config.get("relatedName").asText());
            ps.setString(9, config.get("description").asText());
            ps.setString(10, config.get("defaultValue").asText());
            ps.setString(11, config.get("units").asText());
            ps.setInt(12, (config.get("required").asBoolean()) ? 1 : 0);
            ps.setInt(13, (config.get("sensitive").asBoolean()) ? 1 : 0);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static class ConfigurationHeader {

        private String product;
        private String version;
        private String serviceType;
        private String roleGroupName;
        private String roleGroupCategoryName;

        public ConfigurationHeader(String product) {
            this.product = product;
        }

        public String getProduct() {
            return product;
        }

        public String getVersion() {
            return version;
        }

        public String getServiceType() {
            return serviceType;
        }

        public String getRoleGroupName() {
            return roleGroupName;
        }

        public String getCategoryName() {
            return roleGroupCategoryName;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public void setServiceType(String serviceType) {
            this.serviceType = serviceType;
        }

        public void setRoleGroupName(String roleGroupName) {
            this.roleGroupName = roleGroupName;
        }

        public void setCategoryName(String roleGroupCategoryName) {
            this.roleGroupCategoryName = roleGroupCategoryName;
        }
    }

    public static void main(String[] args) {
        ConfigurationDatabaseLoader loader = new ConfigurationDatabaseLoader();
        try {
            loader.run();
            loader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}

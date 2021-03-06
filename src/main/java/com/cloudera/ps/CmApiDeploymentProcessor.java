package com.cloudera.ps;

import com.cloudera.ps.cm.api.deployment.transform.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class CmApiDeploymentProcessor {

    public CmApiDeploymentProcessor() {
        //
    }

    public void run(CommandLine cmd) throws IOException {
        JsonNode original = readDeploymentJson(cmd);
        ObjectMapper outputMapper = buildOutputMapper(cmd);
        System.out.println(outputMapper.writeValueAsString(runAllTransformers(cmd, original)));
    }

    private ObjectMapper buildOutputMapper(CommandLine cmd) {
        ObjectMapper mapper = new ObjectMapper();
        if (cmd.hasOption("o")) {
            String outputType = cmd.getOptionValue("o");
            if ("xml".equalsIgnoreCase(outputType)) {
                mapper = new XmlMapper();
            } else if ("yaml".equalsIgnoreCase(outputType)) {
                mapper = new YAMLMapper();
            }
        }
        mapper.configure(SerializationFeature.INDENT_OUTPUT, cmd.hasOption("p"));
        return mapper;
    }

    private JsonNode readDeploymentJson(CommandLine cmd) throws IOException {
        if (cmd.hasOption("f") && cmd.hasOption("u")) {
            throw new RuntimeException("Cannot specify both 'file' and 'url' input");
        }
        ObjectMapper mapper = new ObjectMapper();
        if (cmd.hasOption("f")) {
            String inputPath = cmd.getOptionValue("f");
            return mapper.readValue(FileUtils.readFileToString(new File(inputPath)), JsonNode.class);
        } else if (cmd.hasOption("u")) {
            return mapper.readValue(new URL(cmd.getOptionValue("u")), JsonNode.class);
        } else {
            throw new RuntimeException("Must specify one of 'file' or 'url' parameters");
        }
    }

    private JsonNode runAllTransformers(CommandLine cmd, JsonNode output) {
        List<DeploymentTransformer> transformerChain = buildTransformerChain(cmd);
        for (DeploymentTransformer transformer: transformerChain) {
            output = transformer.transform(output);
        }
        return output;
    }

    private List<DeploymentTransformer> buildTransformerChain(CommandLine cmd) {
        List<DeploymentTransformer> transformerChain = new LinkedList<DeploymentTransformer>();

        if (cmd.hasOption("r")) {
            transformerChain.add(new DeploymentReformatter());
        }
        if (cmd.hasOption("s")) {
            transformerChain.add(new ObjectNodeFieldSorter());
        }
        if (cmd.hasOption("a")) {
            // TODO add different enrichment types
            System.out.println(Arrays.toString(cmd.getOptionValues("a")));
            if (!cmd.hasOption("r"))
                throw new RuntimeException("Cannot specify option 'add_api_paths' without option 'reformat'");
            transformerChain.add(new DeploymentApiPathIncluder(getApiVersion(cmd)));
        }
        transformerChain.add(new ObjectNodeFieldFilter());
        return transformerChain;
    }

    private String getApiVersion(CommandLine cmd) {
        if (cmd.hasOption("v"))
            return "v" + cmd.getOptionValue("v");
        return "v10";
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(Option.builder("p")
                .desc("Pretty-print output")
                .longOpt("pretty_print")
                .build());
        options.addOption(Option.builder("r")
                .desc("Reformat to compact output")
                .longOpt("reformat")
                .build());
        options.addOption(Option.builder("a")
                .desc("Add extra information (requires 'reformat')")
                .valueSeparator(',')
                .longOpt("add")
                .build());
        options.addOption(Option.builder("h")
                .desc("Show help screen")
                .longOpt("help")
                .build());
        options.addOption(Option.builder("o")
                .desc("Output format json (default), xml or yaml")
                .longOpt("output-format")
                .hasArg().argName("FORMAT")
                .build());
        options.addOption(Option.builder("s")
                .desc("Sort the json object fields")
                .longOpt("sort")
                .build());
        options.addOption(Option.builder("v")
                .desc("API version to use in output")
                .longOpt("api_version")
                .hasArg().argName("VERSION")
                .build());
        options.addOption(Option.builder("f")
                .desc("Deployment json file to load")
                .longOpt("file")
                .hasArg().argName("FILE")
                .build());
        options.addOption(Option.builder("u")
                .desc("Deployment API url to load")
                .longOpt("url")
                .hasArg().argName("URL")
                .build());
        options.addOption(Option.builder("user")
                .desc("Username for CM API")
                .hasArg().argName("USER")
                .build());
        options.addOption(Option.builder("pass")
                .desc("Password for CM API")
                .hasArg().argName("PASS")
                .build());
        return options;
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("CM API cm/deployment Utils", "", options, "", true);
    }

    public static void main(String[] args) throws IOException {
        Options options = buildOptions();
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse( options, args);
            if (cmd.hasOption("h")) {
                printHelp(options);
                System.exit(0);
            }
            new CmApiDeploymentProcessor().run(cmd);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            printHelp(options);
        }
    }

}

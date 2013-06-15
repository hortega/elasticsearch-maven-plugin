package com.github.elasticsearchmavenplugin;

/*
 * Copyright 2001-2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;
import java.io.IOException;

@Mojo(name = "start",
        defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST)
public class StartElasticsearchNodeMojo extends AbstractMojo {

    @Parameter(property = "clusterName",
            required = true,
            defaultValue = "elasticsearch-test")
    private String clusterName;

    @Parameter(property = "mappingFile",
            required = false)
    private String mappingFile;

    @Parameter(property = "indexName",
            required = true)
    private String indexName;

    @Parameter(property = "typeName",
            required = true)
    private String typeName;

    @Parameter(property = "dataFolder",
            required = false)
    private String dataFolder;

    private Log log;


    public void execute() throws MojoExecutionException {

        log.info("Starting node");

        Node node = NodeBuilder.nodeBuilder().settings(getSettings()).clusterName(clusterName).node().start();
        Client client = node.client();

        putMapping(client);

        getPluginContext().put("node", node);

        log.info("Elasticsearch node has started");

    }

    private Settings getSettings() {
        return ImmutableSettings.settingsBuilder().
                put("path.data", getValidDataFolder()).
                build();
    }

    private void putMapping(Client client) throws MojoExecutionException {
        boolean indexExists = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
        if (!indexExists) {
            log.info(String.format("Index %s does not exist. Creating it", indexName));
            CreateIndexResponse createIndexResponse = client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
        }

        if (StringUtils.isNotBlank(mappingFile)) {
            log.info("Setting index mapping from path " + mappingFile);
            try {
                PutMappingResponse putMappingResponse = client.admin().indices().preparePutMapping(indexName).setType(typeName)
                        .setSource(FileUtils.readFileToString(new File(mappingFile)))
                        .execute()
                        .actionGet();

            } catch (IOException e) {
                throw new MojoExecutionException("Exception trying to read mapping file", e);
            }
        }
    }

    private String getValidDataFolder() {
        if (StringUtils.isBlank(dataFolder)) {
            return System.getProperty("java.io.tmpdir");
        }
        return dataFolder;
    }

    public Log getLog() {
        return log;
    }

    public void setLog(Log log) {
        this.log = log;
    }
}

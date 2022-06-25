/*-
 * #%L
 * athena-aws-cmdb
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
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
 * #L%
 */
package com.amazonaws.athena.connectors.aws.cmdb.tables;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.cloudfront.AmazonCloudFront;
import com.amazonaws.services.cloudfront.model.CacheBehavior;
import com.amazonaws.services.cloudfront.model.DefaultCacheBehavior;
import com.amazonaws.services.cloudfront.model.DistributionSummary;
import com.amazonaws.services.cloudfront.model.ListDistributionsRequest;
import com.amazonaws.services.cloudfront.model.ListDistributionsResult;
import com.amazonaws.services.cloudfront.model.Origin;
import com.amazonaws.services.cloudfront.model.OriginGroup;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps your TargetGroup instances to a table.
 */
public class DistributionTableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private AmazonCloudFront cloudfront;

    public DistributionTableProvider(AmazonCloudFront cloudfront)
    {
        this.cloudfront = cloudfront;
    }

    /**
     * @See TableProvider
     */
    @Override
    public String getSchema()
    {
        return "cloudfront";
    }

    /**
     * @See TableProvider
     */
    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "distributions");
    }

    /**
     * @See TableProvider
     */
    @Override
    public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableName(), SCHEMA);
    }

    /**
     * Calls DescribeDBInstances on the AWS RDS Client returning all DB Instances that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific DB Instance) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        boolean done = false;
        ListDistributionsRequest request = new ListDistributionsRequest();

        // TODO
        // ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("instance_id");
        // if (idConstraint != null && idConstraint.isSingleValue()) {
        //     request.setDBInstanceIdentifier(idConstraint.getSingleValue().toString());
        // }

        while (!done) {
            ListDistributionsResult response = cloudfront.listDistributions(request);

            for (DistributionSummary distribution : response.getDistributionList().getItems()) {
                instanceToRow(distribution, spiller);
            }

            request.setMarker(response.getDistributionList().getNextMarker());

            if (response.getDistributionList().getNextMarker() == null || !queryStatusChecker.isQueryRunning()) {
                done = true;
            }
        }
    }

    /**
     * Maps a DBInstance into a row in our Apache Arrow response block(s).
     *
     * @param instance The DBInstance to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(DistributionSummary instance,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("id", row, instance.getId());
            matched &= block.offerValue("cname", row, instance.getDomainName());
            List<String> domainNames = instance.getAliases().getItems().stream()
                    .collect(Collectors.toList());
            matched &= block.offerComplexValue("domain_names", row, FieldResolver.DEFAULT, domainNames);
            matched &= block.offerValue("ssl_certificate", row, instance.getViewerCertificate().getACMCertificateArn());
            matched &= block.offerValue("ssl_support_method", row, instance.getViewerCertificate().getSSLSupportMethod());
            matched &= block.offerValue("ssl_minimum_protocol", row, instance.getViewerCertificate().getMinimumProtocolVersion());
            List<Object> origins = Lists.newArrayList();
            origins.addAll(instance.getOrigins().getItems());
            origins.addAll(instance.getOriginGroups().getItems());
            matched &= block.offerComplexValue("origins",
                    row,
                    (Field field, Object val) -> {
                        if (field.getName().equals("id")) {
                            if (val instanceof Origin) {
                                return ((Origin) val).getId();
                            }
                            else if (val instanceof OriginGroup) {
                                return ((OriginGroup) val).getId();
                            }
                        }
                        else if (field.getName().equals("domain_name")) {
                            if (val instanceof Origin) {
                                return ((Origin) val).getDomainName();
                            }
                            else if (val instanceof OriginGroup) {
                                return "";
                            }
                        }
                        else if (field.getName().equals("path")) {
                            if (val instanceof Origin) {
                                return ((Origin) val).getOriginPath();
                            }
                            else if (val instanceof OriginGroup) {
                                return "";
                            }
                        }
                        else if (field.getName().equals("members")) {
                            if (val instanceof OriginGroup) {
                                return ((OriginGroup) val).getMembers().getItems().stream()
                                        .map(o -> o.getOriginId())
                                        .collect(Collectors.joining(","));
                            }
                            else if (val instanceof Origin) {
                                return "";
                            }
                        }
                        throw new RuntimeException("Unknown field " + field.getName());
                    }, origins);
            List<Object> cacheBehaviors = Lists.newArrayList();
            cacheBehaviors.addAll(instance.getCacheBehaviors().getItems());
            cacheBehaviors.add(instance.getDefaultCacheBehavior());
            matched &= block.offerComplexValue("cache_behaviours",
                    row,
                    (Field field, Object val) -> {
                        if (field.getName().equals("path_pattern")) {
                            if (val instanceof CacheBehavior) {
                                return ((CacheBehavior) val).getPathPattern();
                            }
                            else if (val instanceof DefaultCacheBehavior) {
                                return "*";
                            }
                        }
                        else if (field.getName().equals("origin_id")) {
                            if (val instanceof CacheBehavior) {
                                return ((CacheBehavior) val).getTargetOriginId();
                            }
                            else if (val instanceof DefaultCacheBehavior) {
                                return ((DefaultCacheBehavior) val).getTargetOriginId();
                            }
                        }
                        else if (field.getName().equals("viewer_policy")) {
                            if (val instanceof CacheBehavior) {
                                return ((CacheBehavior) val).getViewerProtocolPolicy();
                            }
                            else if (val instanceof DefaultCacheBehavior) {
                                return ((DefaultCacheBehavior) val).getViewerProtocolPolicy();
                            }
                        }
                        else if (field.getName().equals("allow_methods")) {
                            if (val instanceof CacheBehavior) {
                                return  ((CacheBehavior) val).getAllowedMethods().getItems().stream()
                                        .collect(Collectors.joining(","));
                            }
                            else if (val instanceof DefaultCacheBehavior) {
                                return  ((DefaultCacheBehavior) val).getAllowedMethods().getItems().stream()
                                        .collect(Collectors.joining(","));
                            }
                        }
                        throw new RuntimeException("Unknown field " + field.getName());
                    }, cacheBehaviors);
            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("id")
                .addStringField("cname")
                .addListField("domain_names", Types.MinorType.VARCHAR.getType())
                .addStringField("ssl_certificate")
                .addStringField("ssl_support_method")
                .addStringField("ssl_minimum_protocol")
                .addField(
                        FieldBuilder.newBuilder("origins", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("origin", Types.MinorType.STRUCT.getType())
                                                .addStringField("id")
                                                .addStringField("domain_name")
                                                .addStringField("path")
                                                .addStringField("members")
                                                .build())
                                .build())
                .addField(
                        FieldBuilder.newBuilder("cache_behaviours", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("cache_behaviour", Types.MinorType.STRUCT.getType())
                                                .addStringField("path_pattern")
                                                .addStringField("origin_id")
                                                .addStringField("viewer_policy")
                                                .addStringField("allow_methods")
                                                .build())
                                .build())
                .addMetadata("id", "CloudFront Distribution Id")
                .build();
    }
}

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
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsResult;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Maps your TargetGroup instances to a table.
 */
public class TgTableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private AmazonElasticLoadBalancing elbv2;

    public TgTableProvider(AmazonElasticLoadBalancing elbv2)
    {
        this.elbv2 = elbv2;
    }

    /**
     * @See TableProvider
     */
    @Override
    public String getSchema()
    {
        return "elb";
    }

    /**
     * @See TableProvider
     */
    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "target_groups");
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
        DescribeTargetGroupsRequest request = new DescribeTargetGroupsRequest();

        // TODO
        // ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("instance_id");
        // if (idConstraint != null && idConstraint.isSingleValue()) {
        //     request.setDBInstanceIdentifier(idConstraint.getSingleValue().toString());
        // }

        while (!done) {
            DescribeTargetGroupsResult response = elbv2.describeTargetGroups(request);

            for (TargetGroup tginstance : response.getTargetGroups()) {
                instanceToRow(tginstance, spiller);
            }

            request.setMarker(response.getNextMarker());

            if (response.getNextMarker() == null || !queryStatusChecker.isQueryRunning()) {
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
    private void instanceToRow(TargetGroup instance,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("tg_name", row, instance.getTargetGroupName());
            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("tg_name")

                .addMetadata("tg_name", "Target Group name")
                .build();
    }
}

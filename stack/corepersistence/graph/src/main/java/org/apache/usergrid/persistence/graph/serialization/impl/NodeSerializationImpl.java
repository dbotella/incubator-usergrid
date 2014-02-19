/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.usergrid.persistence.graph.serialization.impl;


import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;

import org.apache.usergrid.persistence.collection.OrganizationScope;
import org.apache.usergrid.persistence.collection.astyanax.IdRowCompositeSerializer;
import org.apache.usergrid.persistence.collection.astyanax.MultiTennantColumnFamily;
import org.apache.usergrid.persistence.collection.astyanax.MultiTennantColumnFamilyDefinition;
import org.apache.usergrid.persistence.collection.astyanax.ScopedRowKey;
import org.apache.usergrid.persistence.collection.migration.Migration;
import org.apache.usergrid.persistence.collection.mvcc.entity.ValidationUtils;
import org.apache.usergrid.persistence.graph.GraphFig;
import org.apache.usergrid.persistence.graph.serialization.NodeSerialization;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.common.base.Optional;
import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.serializers.BooleanSerializer;


/**
 *
 *
 */
@Singleton
public class NodeSerializationImpl implements NodeSerialization, Migration {


    //Row key by node id.
    private static final IdRowCompositeSerializer ROW_SERIALIZER = IdRowCompositeSerializer.get();

    private static final BooleanSerializer BOOLEAN_SERIALIZER = BooleanSerializer.get();


    /**
     * Columns are always a byte, and the entire value is contained within a row key.  This is intentional
     * This allows us to make heavy use of Cassandra's bloom filters, as well as key caches.
     * Since most nodes will only exist for a short amount of time in this CF, we'll most likely have them in the key
     * cache, and we'll also bounce from the BloomFilter on read.  This means our performance will be no worse
     * than checking a distributed cache in RAM for the existence of a marked node.
     */
    private static final MultiTennantColumnFamily<OrganizationScope, Id, Boolean> GRAPH_DELETE =
            new MultiTennantColumnFamily<OrganizationScope, Id, Boolean>( "Graph_Marked_Nodes",
                    new OrganizationScopedRowKeySerializer<Id>( ROW_SERIALIZER ), BOOLEAN_SERIALIZER );


    protected final Keyspace keyspace;


    @Inject
    public NodeSerializationImpl( final Keyspace keyspace) {
        this.keyspace = keyspace;
    }


    @Override
    public Collection<MultiTennantColumnFamilyDefinition> getColumnFamilies() {
        return Collections.singleton(
                new MultiTennantColumnFamilyDefinition( GRAPH_DELETE, BytesType.class.getSimpleName(),
                        BooleanType.class.getSimpleName(), BytesType.class.getSimpleName() ) );
    }


    @Override
    public MutationBatch mark( final OrganizationScope scope, final Id node, final UUID version ) {
        ValidationUtils.validateOrganizationScope( scope );
        ValidationUtils.verifyIdentity( node );
        ValidationUtils.verifyTimeUuid( version, "version" );

        MutationBatch batch = keyspace.prepareMutationBatch();

        batch.withRow( GRAPH_DELETE, ScopedRowKey.fromKey( scope, node ) ).setTimestamp( version.timestamp() )
             .putColumn( true, version );

        return batch;
    }


    @Override
    public MutationBatch delete( final OrganizationScope scope, final Id node, final UUID version ) {
        ValidationUtils.validateOrganizationScope( scope );
        ValidationUtils.verifyIdentity( node );
        ValidationUtils.verifyTimeUuid( version, "version" );

        MutationBatch batch = keyspace.prepareMutationBatch();

        batch.withRow( GRAPH_DELETE, ScopedRowKey.fromKey( scope, node ) ).setTimestamp( version.timestamp() )
             .deleteColumn( true );

        return batch;
    }


    @Override
    public Optional<UUID> getMaxVersion( final OrganizationScope scope, final Id node ) {
        ValidationUtils.validateOrganizationScope( scope );
        ValidationUtils.verifyIdentity( node );

        ColumnFamilyQuery<ScopedRowKey<OrganizationScope, Id>, Boolean> query = keyspace.prepareQuery( GRAPH_DELETE );


        try {
            Column<Boolean> result =
                    query.getKey( ScopedRowKey.fromKey( scope, node ) ).getColumn( true ).execute().getResult();

            return Optional.of( result.getUUIDValue() );
        }
        catch ( NotFoundException e ) {
            //swallow, there's just no column
            return Optional.absent();
        }
        catch ( ConnectionException e ) {
            throw new RuntimeException( "Unable to connect to casandra", e );
        }
    }
}
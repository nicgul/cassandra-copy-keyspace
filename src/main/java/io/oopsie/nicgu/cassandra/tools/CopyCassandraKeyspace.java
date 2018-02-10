package io.oopsie.nicgu.cassandra.tools;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CopyCassandraKeyspace {
 
    public static void main(String[] args) {
    
        Set<String> argSet = new HashSet(Arrays.asList(args));
        
        String sourceHostArg = argSet.stream().filter(arg -> arg.startsWith("sourceHost=")).findAny().orElse("localhost");
        sourceHostArg = sourceHostArg.replace("sourceHost=", "");
        String[] sourceHostParts = sourceHostArg.split(":");
        String sourceHost = sourceHostParts[0];
        String sourcePort = "9042";
        if(sourceHostParts.length > 1) {
            sourcePort = sourceHostParts[1];
        }
        
        String source = argSet.stream().filter(arg -> arg.startsWith("source=")).findAny().orElse(null);
        if(source == null || source.isEmpty()) {
            System.out.println("The 'source' argument must be specified. [source=<keyspace>]");
        } else {
            source = source.replace("source=", "");
        }
        
        String targetHostArg = argSet.stream().filter(arg -> arg.startsWith("targetHost=")).findAny().orElse("localhost");
        targetHostArg = targetHostArg.replace("targetHost=", "");
        String[] targetHostParts = targetHostArg.split(":");
        String targetHost = targetHostParts[0];
        String targetPort = "9042";
        if(targetHostParts.length > 1) {
            targetPort = targetHostParts[1];
        }
        
        boolean run = false;
        
        String target = argSet.stream().filter(arg -> arg.startsWith("target=")).findAny().orElse(null);
        if(target == null || target.isEmpty()) {
            System.out.println("The 'target' argument must be specified. [target=<keyspace>]");
        } else {
            target = target.replace("target=", "");
            run = true;
        }
        
        if(run) {
            CopyCassandraKeyspace cck = new CopyCassandraKeyspace(sourceHost, sourcePort,
                        source, targetHost, targetPort, target);
            try {
                cck.connect();
                cck.copy();
            } catch(Exception e) {
                System.out.println(e.getMessage());
            } finally {
                cck.close();
            }
        }
    }
    
    private final String sourceHost;
    private final int sourcePort;
    private final String source;
    
    private final String targetHost;
    private final int targetPort;
    private final String target;
    
    private Cluster sourceCluster;
    private Session sourceSession;
    private Cluster targetCluster;
    private Session targetSession;
    
    private Map<String, PreparedStatement> copyPreps = new HashMap();
    
    public CopyCassandraKeyspace(String sourceHost, String sourcePort, String source,
            String targetHost, String targetPort, String target) {
        this.sourceHost = sourceHost == null ? "localhost" : sourceHost;
        this.sourcePort = sourcePort == null ? 9042 : Integer.valueOf(sourcePort);
        
        if(source == null || source.isEmpty()) {
            throw new IllegalArgumentException("The 'source' argument can't be empty.");
        }
        this.source = source;
        this.targetHost = targetHost == null ? "localhost" : targetHost;
        this.targetPort = targetPort == null ? 9042 : Integer.valueOf(targetPort);
        
        if(target == null || target.isEmpty()) {
            throw new IllegalArgumentException("The 'target' argument can't be empty.");
        }
        this.target = target;
    }
    
    public void connect() {
        connectSource();
        connectTarget();
    }
    
    private void connectSource() {
        sourceCluster = Cluster.builder().addContactPoint(sourceHost).withPort(sourcePort).build();
        sourceSession = sourceCluster.connect(source);
        System.out.println("Connected to source cluster: '" + sourceHost + "'");
    }
    
    private void connectTarget() {
        targetCluster = Cluster.builder().addContactPoint(targetHost).withPort(targetPort).build();
        targetSession = targetCluster.connect();
        System.out.println("Connected to target cluster: '" + targetHost + "'");
    }
    
    public void close() {
        closeSource();
        closeTarget();
    }
    
    private void closeSource() {
        if(sourceSession != null) {
            sourceSession.close();
        }
        
        if(sourceCluster != null) {
            sourceCluster.close();
            System.out.println("Closed connection to source cluster: '" + sourceHost + "'");
        }
    }
    
    private void closeTarget() {
        if(targetSession != null) {
            targetSession.close();
        }
        
        if(targetCluster != null) {
            targetCluster.close();
            System.out.println("Closed connection to target cluster: '" + targetHost + "'");
        }
    }
    
    public void copy() {
        copyKeyspace();
    }
    
    private void copyKeyspace() {

        List<String> exportedCqls = Arrays.asList(
                sourceCluster.getMetadata().getKeyspace(source).exportAsString()
                    .replace(System.getProperty("line.separator"), "")
                    .split(";"));
        
        List<String> cqls = new ArrayList();
        exportedCqls.forEach(cql -> {
            if(cql.startsWith("CREATE KEYSPACE")) {
                cqls.add(cql.replace("CREATE KEYSPACE " + source, "CREATE KEYSPACE " + target));
            } else {
                cqls.add(cql.replace(source + ".", target + "."));
            }
        });

        cqls.forEach(cql -> { 
            targetSession.execute(cql);
        });
        System.out.println("Target keyspace created: '" + target + "'");
        copyTables();
    }
    
    
    private void copyTables() {
        
        String udtNameCql = "SELECT type_name FROM system_schema.types WHERE keyspace_name='" + source + "'";
        Set<String> udts = sourceSession.execute(udtNameCql).all().stream()
                .map(row -> row.getString("type_name")).collect(Collectors.toSet());
        
        String tableNameCql = "SELECT table_name FROM system_schema.tables WHERE keyspace_name='" + source + "'";
        Set<String> tables = sourceSession.execute(tableNameCql).all().stream()
                .map(row -> row.getString("table_name")).collect(Collectors.toSet());
        
        tables.forEach(table -> {
            copyTableData(table, udts);
            System.out.println("Copied data from table: '" + table + "'");
        });
    }
    
    private void copyTableData(String table, Set<String> udts) {
        
        String colCql = "SELECT * FROM system_schema.columns WHERE keyspace_name='" + source + "' AND table_name='" + table + "'";
        ResultSet colRs = sourceSession.execute(colCql);

        Map<String, String> columns = Maps.newLinkedHashMap();
        colRs.all().forEach(row -> {
            columns.put(row.getString("column_name"), row.getString("type"));
        });
        String insertColNames = String.join("", columns.keySet().stream().map(c -> c + ",").collect(Collectors.toList()));
        insertColNames = insertColNames.substring(0, insertColNames.lastIndexOf(","));

        List<String> insertPlaceholderList = columns.keySet().stream().map(c -> "?,").collect(Collectors.toList());
        String insertPlaceholders = String.join("", insertPlaceholderList);
        insertPlaceholders = insertPlaceholders.substring(0, insertPlaceholders.lastIndexOf(","));

        String fromCql = String.join("", "SELECT * FROM ", source, ".", table);
        ResultSet fromRs = sourceSession.execute(fromCql);

        String toCql = String.join("",
                "INSERT INTO ",
                target,
                ".",
                table, " (",
                insertColNames,
                ") VALUES (",
                insertPlaceholders,
                 ")");
        PreparedStatement pStmnt = copyPreps.get(toCql);
        if(pStmnt == null) {
            pStmnt = targetSession.prepare(toCql);
            copyPreps.put(toCql, pStmnt);
        }
        for (Row row : fromRs.all()) {
            AtomicInteger counter = new AtomicInteger();
            Object[] values = new Object[columns.size()];
            for (String col : columns.keySet()) {
                String udt = columns.get(col);
                values[counter.getAndIncrement()] = convertUDTValueIfNecessary(row.getObject(col));
            }
            targetSession.executeAsync(pStmnt.bind(values));

            // Give the cassandra driver some room to work asynchronous ..
            try {
                Thread.sleep(5);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    /**
     * Converts a source UDTValue to the target equivalent. If sourceVal is null or not an
     * instance of UDTValue then this method will just return the value as is.
     * 
     * @param sourceVal the value to convert
     * @param session
     * @param toKeyspace
     * @return 
     */
    private Object convertUDTValueIfNecessary(Object sourceVal){
        
        if(sourceVal == null) {
            return null;
        }
        Object newVal = null;
        if(sourceVal instanceof UDTValue) {
            UDTValue udtVal = ((UDTValue)sourceVal);
            UDTValue newUDT = getNewTargetUDTValue(udtVal.getType().getTypeName());
            CodecRegistry codecs = sourceCluster.getConfiguration().getCodecRegistry();
            UserType type = udtVal.getType();
            for(String fn : type.getFieldNames()) {
                Object obj = udtVal.get(fn, codecs.codecFor(type.getFieldType(fn)));
                newUDT.set(fn, obj, codecs.codecFor(type.getFieldType(fn)));
            }
            newVal = newUDT;
        } else if(sourceVal instanceof List) {
            List list = Lists.newArrayList();
            ((List)sourceVal).forEach(v -> {
                list.add(convertUDTValueIfNecessary(v));
            });
            newVal = list;
        } else if(sourceVal instanceof Set) {
            Set set = Sets.newHashSet();
            ((Set)sourceVal).forEach(v -> {
                set.add(convertUDTValueIfNecessary(v));
            });
            newVal = set;
        } else if(sourceVal instanceof Map) {
            Map map = Maps.newHashMap();
            ((Map)sourceVal).forEach((k,v) -> {
                map.put(k, convertUDTValueIfNecessary(v));
            });
            newVal = map;
        } else {
            newVal = sourceVal;
        }
        return newVal;
    }
    
    private UDTValue getNewTargetUDTValue(String udtName) {
        
        UserType userType = targetCluster.getMetadata().getKeyspace(target).getUserTypes()
                .stream().filter(t -> t.getTypeName().equals(udtName)).findAny().get();
        
        return userType.newValue();
    }

}

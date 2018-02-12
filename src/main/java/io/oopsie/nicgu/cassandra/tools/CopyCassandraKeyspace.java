package io.oopsie.nicgu.cassandra.tools;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
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

/**
 * CopyCassandraKeyspace is a convenient tool capabale of recreate the structure
 * and copy the data from one keyspace (source) residing on the passed in
 * sourceHost (default is localhost) into another keyspace (target). The target keyspace will be created
 * in the cluster residing on passed in targetHost (default is localhost).
 */
public class CopyCassandraKeyspace {
 
    /**
     * <p>
     * Mandatory parameters: source=source-keyspace target=target-keyspace
     * <p>
     * Optional parameters: [sourceHost=host[:port]] [targetHost=host[:port]] [sourceCreds=username::password] [targetCreds=username::password]
     * @param args the mandatoru and optional params mentioned in metod javadocs.
     */
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
        
        String sourceCredsArg = argSet.stream().filter(arg -> arg.startsWith("sourceCreds=")).findAny().orElse("");
        sourceCredsArg = sourceCredsArg.replace("sourceCreds=", "");
        String[] sourceCredsParts = sourceCredsArg.split("::");
        String sourceUser = sourceCredsParts[0];
        String sourcePass = "";
        if(sourceCredsParts.length > 1) {
            sourcePass = sourceCredsParts[1];
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
        String targetPort = "";
        if(targetHostParts.length > 1) {
            targetPort = targetHostParts[1];
        }
        
        String targetCredsArg = argSet.stream().filter(arg -> arg.startsWith("targetCreds=")).findAny().orElse("");
        targetCredsArg = targetCredsArg.replace("targetCreds=", "");
        String[] targetCredsParts = targetCredsArg.split("::");
        String targetUser = targetCredsParts[0];
        String targetPass = "";
        if(targetCredsParts.length > 1) {
            targetPass = targetCredsParts[1];
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
            CopyCassandraKeyspace cck = new CopyCassandraKeyspace(
                    sourceHost,
                    sourcePort,
                    source,
                    sourceUser,
                    sourcePass,
                    targetHost,
                    targetPort,
                    target,
                    targetUser,
                    targetPass
            );
            try {
                cck.connect();
                cck.copy();
            } catch(Exception e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
            } finally {
                cck.close();
            }
        }
    }
    
    private final String sourceHost;
    private final int sourcePort;
    private final String source;
    private final String sourceUser;
    private final String sourcePass;
    
    private final String targetHost;
    private final int targetPort;
    private final String target;
    private final String targetUser;
    private final String targetPass;
    
    private Cluster sourceCluster;
    private Session sourceSession;
    private Cluster targetCluster;
    private Session targetSession;
    
    private Map<String, PreparedStatement> copyPreps = new HashMap();
    
    /**
     * Create a new CopyCassandraKeyspace instance capable of copying data from source to target.
     * 
     * @param sourceHost the host where the source keyspace resides
     * @param sourcePort the port of the host where the source keyspace resides
     * @param source the name of the source keyspace
     * @param sourceUser the username of the source keyspace cluster
     * @param sourcePass the password of the source keyspace cluster
     * @param targetHost the host where the target keyspace resides
     * @param targetPort the port of the host where the target keyspace resides
     * @param target the name of the target keyspace
     * @param targetUser the username of the target keyspace cluster
     * @param targetPass the password of the target keyspace cluster
     */
    public CopyCassandraKeyspace(String sourceHost, String sourcePort, String source, String sourceUser, String sourcePass,
            String targetHost, String targetPort, String target, String targetUser, String targetPass) {
        
        if(source == null || source.trim().isEmpty()) {
            throw new IllegalArgumentException("The 'source' argument can't be empty.");
        }
        this.source = source;
        if(target == null || target .trim().isEmpty()) {
            throw new IllegalArgumentException("The 'target' argument can't be empty.");
        }
        this.target = target;
        
        this.sourceHost = sourceHost == null || sourceHost.trim().isEmpty() ? "localhost" : sourceHost.trim();
        this.sourcePort = sourcePort == null || sourcePort.trim().isEmpty() ? 9042 : Integer.valueOf(sourcePort.trim());
        this.sourceUser = sourceUser == null || sourceUser.trim().isEmpty() ? null : sourceUser.trim();
        this.sourcePass = sourcePass == null || sourcePass.trim().isEmpty() ? null : sourcePass.trim();
        
        this.targetHost = targetHost == null || targetHost.trim().isEmpty() ? "localhost" : targetHost.trim();
        this.targetPort = targetPort == null || targetPort.trim().isEmpty() ? 9042 : Integer.valueOf(targetPort.trim());
        this.targetUser = targetUser == null || targetUser.trim().isEmpty() ? null : targetUser.trim();
        this.targetPass = targetPass == null || targetPass.trim().isEmpty() ? null : targetPass.trim();
        
    }
    
    /**
     * Connects the source and the target Cassandra cluster and session objects.
     * Call this method before calling {@link #copy()}.
     * 
     * @see #copy() 
     */
    public void connect() {
        connectSource();
        connectTarget();
    }
    
    /**
     * Connects the source cluster and session objects.
     */
    private void connectSource() {
        
        sourceCluster = Cluster.builder()
                .addContactPoint(sourceHost).withPort(sourcePort)
                .withCredentials(sourceUser, sourcePass)
                .build();
        sourceSession = sourceCluster.connect(source);
        System.out.println("Connected to source cluster: '" + sourceHost + "'");
    }
    
    /**
     * Connects the target cluster and session objects.
     */
    private void connectTarget() {
        targetCluster = Cluster.builder().addContactPoint(targetHost).withPort(targetPort)
                .withCredentials(targetUser, targetPass)
                .build();
        targetSession = targetCluster.connect();
        System.out.println("Connected to target cluster: '" + targetHost + "'");
    }
    
    /**
     * Closes the source and the target Cassandra cluster and session objects.
     */
    public void close() {
        closeSource();
        closeTarget();
    }
    
    /**
     * Close the source cluster and session objects.
     */
    private void closeSource() {
        if(sourceSession != null) {
            sourceSession.close();
        }
        
        if(sourceCluster != null) {
            sourceCluster.close();
            System.out.println("Closed connection to source cluster: '" + sourceHost + "'");
        }
    }
    
    /**
     * Close the target cluster and session objects.
     */
    private void closeTarget() {
        if(targetSession != null) {
            targetSession.close();
        }
        
        if(targetCluster != null) {
            targetCluster.close();
            System.out.println("Closed connection to target cluster: '" + targetHost + "'");
        }
    }
    
    /**
     * Starts the copying process. Prior to calling this method
     * {@link #connect()} must be called.
     * 
     * @see #connect() 
     */
    public void copy() {
        copyKeyspace();
    }
    
    /**
     * Fetches the all CQLs needed to recreate the source keyspace structure and exeecutes
     * these to create the target keyspace.
     */
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
    
    /**
     * Convenient method that loops through all table in source keyspace to be copied.
     */
    private void copyTables() {
        
        String tableNameCql = "SELECT table_name FROM system_schema.tables WHERE keyspace_name='" + source + "'";
        Set<String> tables = sourceSession.execute(tableNameCql).all().stream()
                .map(row -> row.getString("table_name")).collect(Collectors.toSet());
        
        tables.forEach(table -> {
            copyTableData(table);
        });
    }
    
    /**
     * Selects data from passed in table in the source keyspace and isnerts hte data
     * into a table with same name in the target keyspace.
     * 
     * @param table the name of the table to copy data from
     */
    private void copyTableData(String table) {
        
        List<String> setColParams = new ArrayList();
        List<ColumnMetadata> setCols = new ArrayList();
        List<String> whereColParams = new ArrayList();
        List<ColumnMetadata> whereCols = sourceCluster.getMetadata().getKeyspace(source)
                .getTable(table) .getPrimaryKey();
        List<ColumnMetadata> cols = sourceCluster.getMetadata().getKeyspace(source)
                .getTable(table).getColumns();

        whereCols.forEach(cmd -> {
            whereColParams.add(ParseUtils.doubleQuote(cmd.getName()) + "=?");
        });

        cols.forEach(cmd -> {
            if(!whereCols.contains(cmd)) {
                if(cmd.getType().getName().equals(DataType.Name.COUNTER)) {
                    setColParams.add(ParseUtils.doubleQuote(cmd.getName()) + "=" + ParseUtils.doubleQuote(cmd.getName()) + "+?");
                } else {
                    setColParams.add(ParseUtils.doubleQuote(cmd.getName()) + "=?");
                }
                setCols.add(cmd);
            }
        });

        String fromCql = String.join("", "SELECT * FROM ", source, ".", table);
        ResultSet fromRs = sourceSession.execute(fromCql);
        String cql;
        List<ColumnMetadata> execCols = new ArrayList();
        if(setCols.isEmpty()) {


            String insertColNames = String.join("", cols.stream().map(c -> ParseUtils.doubleQuote(c.getName()) + ",").collect(Collectors.toList()));
            insertColNames = insertColNames.substring(0, insertColNames.lastIndexOf(","));

            List<String> insertPlaceholderList = cols.stream().map(c -> "?,").collect(Collectors.toList());
            String insertPlaceholders = String.join("", insertPlaceholderList);
            insertPlaceholders = insertPlaceholders.substring(0, insertPlaceholders.lastIndexOf(","));

            String insertCql = String.join("",
                    "INSERT INTO ",
                    target,
                    ".",
                    table, " (",
                    insertColNames,
                    ") VALUES (",
                    insertPlaceholders,
                     ")");
            cql = insertCql;
            execCols.addAll(cols);
        } else {

            String setParams = String.join("", setColParams.stream().map(c -> c + ",").collect(Collectors.toList()));
            setParams = setParams.substring(0, setParams.lastIndexOf(","));

            String whereParams = String.join("", whereColParams.stream().map(c -> c + " AND ").collect(Collectors.toList()));
            whereParams = whereParams.substring(0, whereParams.lastIndexOf(" AND "));
            String updateCql = String.join("",
                    "UPDATE ",
                    target,
                    ".",
                    table,
                    " SET ",
                    setParams,
                    " WHERE ",
                    whereParams);
            cql= updateCql;
            execCols.addAll(setCols);
            execCols.addAll(whereCols);
        }

        PreparedStatement pStmnt = copyPreps.get(cql);
        if(pStmnt == null) {
            pStmnt = targetSession.prepare(cql);
            copyPreps.put(cql, pStmnt);
        }

        for (Row row : fromRs.all()) {
            AtomicInteger counter = new AtomicInteger();
            Object[] values = new Object[setColParams.size() + whereColParams.size()];
            for (ColumnMetadata execCol : execCols) {
                values[counter.getAndIncrement()] = convertUDTValueIfNecessary(row.getObject(execCol.getName()));
            }
            targetSession.executeAsync(pStmnt.bind(values));

            // Give the cassandra driver some room to work asynchronous ..
            // .. otherwise we eventually might get a NoHostAvailableException ....
            try {
                Thread.sleep(1);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Copied data from table: '" + table + "'");
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
        
        CodecRegistry sourceCodecs = sourceCluster.getConfiguration().getCodecRegistry();
        CodecRegistry targetCodecs = targetCluster.getConfiguration().getCodecRegistry();
        Object targetVal;
        if(sourceVal instanceof UDTValue) {
            UDTValue sourceUDTVal = ((UDTValue)sourceVal);
            UDTValue targetUDTVal = getNewTargetUDTValue(sourceUDTVal.getType().getTypeName());
            UserType type = sourceUDTVal.getType();
            type.getFieldNames().forEach((fn) -> {
                Object val = sourceUDTVal.get(fn, sourceCodecs.codecFor(type.getFieldType(fn)));
                targetUDTVal.set(fn, val, targetCodecs.codecFor(type.getFieldType(fn)));
            });
            targetVal = targetUDTVal;
        }  else if(sourceVal instanceof TupleValue) {
            TupleValue sourceTuple = (TupleValue)sourceVal;
            TupleValue targetTuple = getNewTargetTupleValue(sourceTuple);
            List<DataType> compTypes = targetTuple.getType().getComponentTypes();
            AtomicInteger counter = new AtomicInteger(0);
            compTypes.forEach(ct -> {
                Object val = convertUDTValueIfNecessary(sourceTuple.get(counter.get(), targetCodecs.codecFor(ct)));
                targetTuple.set(counter.getAndIncrement(), val, targetCodecs.codecFor(ct));
            });
            targetVal = targetTuple;
        } else if(sourceVal instanceof List) {
            List list = Lists.newArrayList();
            ((List)sourceVal).forEach(v -> {
                list.add(convertUDTValueIfNecessary(v));
            });
            targetVal = list;
        } else if(sourceVal instanceof Set) {
            Set set = Sets.newHashSet();
            ((Set)sourceVal).forEach(v -> {
                set.add(convertUDTValueIfNecessary(v));
            });
            targetVal = set;
        } else if(sourceVal instanceof Map) {
            Map map = Maps.newHashMap();
            ((Map)sourceVal).forEach((k,v) -> {
                map.put(k, convertUDTValueIfNecessary(v));
            });
            targetVal = map;
        } else {
            targetVal = sourceVal;
        }
        return targetVal;
    }
    
    /**
     * Returns a new target UDTValue with a codec fitting the target keyspace.
     * 
     * @param udtName the name of the UDT.
     * @return a new target keyspace UDTValue.
     */
    private UDTValue getNewTargetUDTValue(String udtName) {
        UserType userType = targetCluster.getMetadata().getKeyspace(target).getUserTypes()
                .stream().filter(t -> t.getTypeName().equals(udtName)).findAny().get();
        return userType.newValue();
    }
    
    /**
     * Takes a TupleVale from the source keyspace and creates a TupleValue
     * with a codec fitting the target keyspace.
     * 
     * @param sourceVal the source tuplevale
     * @return a new TupleValue with codec fitting the target keyspace.
     */
    private TupleValue getNewTargetTupleValue(TupleValue sourceVal) {
        
        List<DataType> types = new ArrayList();
        for (DataType ct : sourceVal.getType().getComponentTypes()) {
            types.add(getTargetType(ct));
        }
        return targetCluster.getMetadata().newTupleType(types).newValue();
    }
    
    /**
     * Converts a list of source DataTypes to a list of types with a codec fitting the target keyspace.
     * 
     * @param sourceType the source keyspace DataTypes
     * @return DataTypes with target codec.
     */
    private List<DataType> getTragetTypes(List<DataType> sourceTypes) {
        
        List<DataType> targetTypes = new ArrayList();
        for (DataType st : sourceTypes) {
            targetTypes.add(getTargetType(st));
        }
        return targetTypes;
    }
    
    /**
     * Converts a source DataType to a type with a codec fitting the target keyspace.
     * 
     * @param sourceType the source keyspace DataType
     * @return a DataType with target codec.
     */
    private DataType getTargetType(DataType sourceType) {
        
        DataType targetType;
        
        switch(sourceType.getName()) {
                case ASCII:
                    targetType = DataType.ascii();
                    break;
                case BIGINT:
                    targetType = DataType.bigint();
                    break;
                case BLOB:
                    targetType = DataType.blob();
                    break;
                case BOOLEAN:
                    targetType = DataType.cboolean();
                    break;
                case COUNTER:
                    targetType = DataType.counter();
                    break;
                case CUSTOM:
                    String customTypeName = ((DataType.CustomType)sourceType).getCustomTypeClassName();
                    targetType = DataType.custom(customTypeName);
                    break;
                case DATE:
                    targetType = DataType.date();
                    break;
                case DECIMAL:
                    targetType = DataType.decimal();
                    break;
                case DOUBLE:
                    targetType = DataType.cdouble();
                    break;
                case DURATION:
                    targetType = DataType.duration();
                    break;
                case FLOAT:
                    targetType = DataType.cfloat();
                    break;
                case INET:
                    targetType = DataType.inet();
                    break;
                case INT:
                    targetType = DataType.cint();
                    break;
                case LIST:
                    if(sourceType.isFrozen()) {
                        targetType = DataType.frozenList(getTargetType(sourceType.getTypeArguments().get(0)));
                    } else {
                        targetType = DataType.list(getTargetType(sourceType.getTypeArguments().get(0)));
                    }
                    break;
                case MAP:
                    if(sourceType.isFrozen()) {
                        targetType = DataType.frozenMap(getTargetType(sourceType.getTypeArguments().get(0)),
                                getTargetType(sourceType.getTypeArguments().get(1)));
                    } else {
                        targetType = DataType.map(getTargetType(sourceType.getTypeArguments().get(0)),
                                getTargetType(sourceType.getTypeArguments().get(1)));
                    }
                    break;
                case SET:
                    if(sourceType.isFrozen()) {
                        targetType = DataType.frozenSet(getTargetType(sourceType.getTypeArguments().get(0)));
                    } else {
                        targetType = DataType.set(getTargetType(sourceType.getTypeArguments().get(0)));
                    }
                    break;
                case SMALLINT:
                    targetType = DataType.smallint();
                    break;
                case TEXT:
                    targetType = DataType.text();
                    break;
                case TIME:
                    targetType = DataType.time();
                    break;
                case TIMESTAMP:
                    targetType = DataType.timestamp();
                    break;
                case TIMEUUID:
                    targetType = DataType.timeuuid();
                    break;
                case TINYINT:
                    targetType = DataType.tinyint();
                    break;
                case TUPLE:
                    List<DataType> tupleComps = getTragetTypes(((TupleType)sourceType).getComponentTypes());
                     TupleType tupleType = targetCluster.getMetadata().newTupleType(tupleComps);
                    targetType = tupleType;
                    break;
                case UDT:
                    String typeName = ((UserType)sourceType).getTypeName();
                    UserType userType = targetCluster.getMetadata().getKeyspace(target).getUserType(typeName);
                    targetType = userType;
                    break;
                case UUID:
                    targetType = DataType.uuid();
                    break;
                case VARCHAR:
                    targetType = DataType.varchar();
                    break;
                case VARINT:
                    targetType = DataType.varint();
                    break;
                default:
                    throw new IllegalArgumentException("Can't find the passed in type");
            }
        return targetType;
    }
}

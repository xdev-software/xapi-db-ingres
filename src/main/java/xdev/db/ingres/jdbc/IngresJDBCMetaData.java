/*
 * SqlEngine Database Adapter Ingres - XAPI SqlEngine Database Adapter for Ingres
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.ingres.jdbc;




import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.StoredProcedure;
import xdev.db.StoredProcedure.Param;
import xdev.db.StoredProcedure.ParamType;
import xdev.db.StoredProcedure.ReturnTypeFlavor;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCDataSource;
import xdev.db.jdbc.JDBCMetaData;
import xdev.util.CollectionUtils;
import xdev.util.ProgressMonitor;
import xdev.util.Settings;
import xdev.util.StringUtils;
import xdev.vt.Cardinality;
import xdev.vt.EntityRelationship;
import xdev.vt.EntityRelationship.Entity;
import xdev.vt.EntityRelationshipModel;
import xdev.vt.VirtualTable;
import xdev.vt.VirtualTable.VirtualTableRow;

import com.ingres.gcf.jdbc.JdbcRslt;
import com.ingres.gcf.util.IdMap;
import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;


public class IngresJDBCMetaData extends JDBCMetaData
{
	private static final IdMap[]	dataTypeMap	= {
		new IdMap(30,"tinyint"),
		new IdMap(30,"smallint"),
		new IdMap(30,"integer"),
		new IdMap(30,"int"),
		new IdMap(30,"bigint"),
		new IdMap(31,"real"),
		new IdMap(31,"float"),
		new IdMap(31,"double precision"),
		new IdMap(31,"double p"),
		new IdMap(10,"decimal"),
		new IdMap(10,"numeric"),
		new IdMap(20,"char"),
		new IdMap(20,"character"),
		new IdMap(21,"varchar"),
		new IdMap(22,"long varchar"),
		new IdMap(26,"nchar"),
		new IdMap(27,"nvarchar"),
		new IdMap(28,"long nvarchar"),
		new IdMap(23,"byte"),
		new IdMap(24,"varbyte"),
		new IdMap(24,"byte varying"),
		new IdMap(25,"long byte"),
		new IdMap(32,"c"),
		new IdMap(37,"text"),
		new IdMap(5,"money"),
		new IdMap(38,"boolean"),
		new IdMap(3,"date"),
		new IdMap(3,"ingresdate"),
		new IdMap(4,"ansidate"),
		new IdMap(8,"time with local time zone"),
		new IdMap(6,"time without time zone"),
		new IdMap(7,"time with time zone"),
		new IdMap(19,"timestamp with local time zone"),
		new IdMap(9,"timestamp without time zone"),
		new IdMap(18,"timestamp with time zone"),
		new IdMap(21,"interval year to month"),
		new IdMap(21,"interval day to second")
	};
	
	
	public IngresJDBCMetaData(IngresJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	protected String getCatalog(JDBCDataSource dataSource)
	{
		return null;
	}
	
	
	@Override
	protected String getSchema(JDBCDataSource dataSource)
	{
		String schema = super.getSchema(dataSource);
		if(schema == null || schema.length() == 0)
		{
			schema = dataSource.getUserName().toUpperCase();
		}
		
		return schema;
	}
	
	
	@Override
	public TableInfo[] getTableInfos(ProgressMonitor monitor, EnumSet<TableType> types)
			throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<TableInfo> list = new ArrayList<>();
		
		try(JDBCConnection jdbcConnection = (JDBCConnection)dataSource.openConnection())
		{
			String schema = getSchema(dataSource);
			
			boolean tables = types.contains(TableType.TABLE);
			boolean views = types.contains(TableType.VIEW);
			String s = null;
			
			if(tables && views)
			{
				s = "table_type in('T','V')";
			}
			else if(tables)
			{
				s = "table_type='T'";
			}
			else if(views)
			{
				s = "table_type='V'";
			}
			if(s != null)
			{
				Result result = jdbcConnection.query("SELECT table_name, table_type FROM iitables "
					+ "where system_use<>'S' and table_name not like 'ii%' and " + s
					+ " order by table_name");
				while(result.next() && !monitor.isCanceled())
				{
					String name = result.getString("table_name").trim();
					TableType type = result.getString("table_type").equals("T") ? TableType.TABLE
						: TableType.VIEW;
					TableInfo tableInfo = new TableInfo(type, schema, name);
					list.add(tableInfo);
				}
				result.close();
			}
		}
		
		monitor.done();
		
		TableInfo[] tables = list.toArray(new TableInfo[list.size()]);
		Arrays.sort(tables);
		return tables;
	}
	
	
	@Override
	public TableMetaData[] getTableMetaData(ProgressMonitor monitor, int flags, TableInfo... tables)
			throws DBException
	{
		boolean trim = Settings.trimData();
		Settings.setTrimData(true);
		
		try
		{
			return getTableMetaData0(monitor,flags,tables);
		}
		finally
		{
			Settings.setTrimData(trim);
		}
	}
	
	
	private TableMetaData[] getTableMetaData0(ProgressMonitor monitor, int flags,
			TableInfo... tables) throws DBException
	{
		if(tables == null || tables.length == 0)
		{
			return new TableMetaData[0];
		}
		
		List<TableMetaData> list = new ArrayList<>(tables.length);
		
		try(JDBCConnection jdbcConnection = (JDBCConnection)dataSource.openConnection())
		{
			monitor.beginTask("", ProgressMonitor.UNKNOWN);
			
			List<String> params = new ArrayList<>();
			params.add(dataSource.getUserName());
			
			String sbColumns = "select * " +
				"from iicolumns " +
				"where table_owner=? and table_name not like '$%' " +
				"order by table_name, column_sequence";
			
			Result result = jdbcConnection.query(sbColumns, params.toArray());
			VirtualTable vtColumns = new VirtualTable(result, true);
			result.close();
			
			Map<String, List<VirtualTableRow>> columnMap = toMap(vtColumns, "table_name");
			Map<String, List<VirtualTableRow>> primaryKeyMap = null;
			Map<String, List<VirtualTableRow>> indexMap = null;
			
			if((flags & INDICES) != 0 && !monitor.isCanceled())
			{
				String sbPrimaryKeys =
					"select distinct k.schema_name, k.table_name, k.column_name, " + "k.key_position, k.constraint_name " +
						"from iikeys k, iiconstraints c " +
						"where c.constraint_type = 'P'  and k.constraint_name = c.constraint_name " +
						"and k.schema_name=? order by table_name, key_position";
				
				Result rs = jdbcConnection.query(sbPrimaryKeys, params.toArray());
				VirtualTable vtPrimaryKeys = new VirtualTable(rs, true);
				rs.close();
				primaryKeyMap = toMap(vtPrimaryKeys, "table_name");
				
				String sbIndexes = "select idx.base_owner, idx.base_name, idx.unique_rule, " +
						"idx.index_owner, idx.index_name, idc.key_sequence, idc.column_name, " +
						"idc.sort_direction " +
					"from iiindexes idx, iiindex_columns idc " +
					"where idx.index_owner = idc.index_owner and idx.index_name = idc.index_name " +
						"and idx.unique_rule = 'U' " + // fetch only
						// unique
						// indexes
						"and idx.base_owner=? " +
					"order by 3 desc, 5, 6";
				
				rs = jdbcConnection.query(sbIndexes, params.toArray());
				VirtualTable vtIndexes = new VirtualTable(rs, true);
				rs.close();
				indexMap = toMap(vtIndexes, "base_name");
			}
			
			Map<Object, Object> defaultValueMap = new HashMap<>();
			
			if(!monitor.isCanceled())
			{
				Set<Object> defaultValueSet = new HashSet<>();
				int rc = vtColumns.getRowCount();
				int hasDefaultIndex = vtColumns.getColumnIndex("column_has_default");
				int columnDefaultIndex = vtColumns.getColumnIndex("column_default_val");
				for(int i = 0; i < rc; i++)
				{
					if(hasDefaultIndex == -1 || "Y".equals(vtColumns.getValueAt(i, hasDefaultIndex)))
					{
						Object defaultValue = vtColumns.getValueAt(i, columnDefaultIndex);
						if(defaultValue != null
							&& !defaultValue.toString().startsWith("next value for"))
						{
							if("".equals(defaultValue))
							{
								defaultValueSet.add("''");
							}
							else
							{
								defaultValueSet.add(defaultValue);
							}
						}
					}
				}
				
				if(!defaultValueSet.isEmpty())
				{
					Object[] defaultValues = defaultValueSet.toArray();
					
					StringBuilder sbDefaultValues = new StringBuilder("SELECT ");
					sbDefaultValues.append(StringUtils.concat(",", defaultValues));
					
					try
					{
						result = jdbcConnection.query(sbDefaultValues.toString());
						try
						{
							result.next();
							for(int i = 0, cc = result.getColumnCount(); i < cc; i++)
							{
								defaultValueMap.put(defaultValues[i], result.getObject(i));
							}
						}
						finally
						{
							result.close();
						}
					}
					catch(Exception e)
					{
						System.err.println("Ingres: Error retrieving column default values:");
						e.printStackTrace();
					}
				}
			}
			
			monitor.beginTask("", tables.length);
			
			int done = 0;
			for(TableInfo table : tables)
			{
				if(monitor.isCanceled())
				{
					break;
				}
				
				monitor.setTaskName(table.getName());
				try
				{
					list.add(getTableMetaData(table, flags, columnMap, primaryKeyMap, indexMap,
						defaultValueMap));
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				monitor.worked(++done);
			}
		}
		
		monitor.done();
		
		return list.toArray(new TableMetaData[list.size()]);
	}
	
	
	private TableMetaData getTableMetaData(
		TableInfo table,
		int flags,
		Map<String, List<VirtualTableRow>> columnMap,
		Map<String, List<VirtualTableRow>> primaryKeyMap,
		Map<String, List<VirtualTableRow>> indexMap,
		Map<Object, Object> defaultValueMap)
			throws Exception
	{
		int column_bydefault_ident_index = -2;
		
		String tableName = table.getName();
		List<VirtualTableRow> columnRows = columnMap.get(tableName);
		int columnCount = columnRows.size();
		ColumnMetaData[] columns = new ColumnMetaData[columnCount];
		for(int i = 0; i < columnCount; i++)
		{
			VirtualTableRow dataRow = columnRows.get(i);
			String columnName = (String)dataRow.get("column_name");
			String caption = null;
			
			int ingresType = ((Number)dataRow.get("column_ingdatatype")).intValue();
			if(ingresType != -1)
			{
				ingresType = Math.abs(ingresType);
			}
			else
			{
				String typeName = ((String)dataRow.get("column_datatype")).toLowerCase();
				ingresType = IdMap.get(typeName,dataTypeMap);
			}
			
			int length = ((Number)dataRow.get("column_length")).intValue();
			int scale = ((Number)dataRow.get("column_scale")).intValue();
			
			int sqlType = convToJavaType(ingresType,length);
			DataType type = DataType.get(sqlType);
			
			int colSize = colSize(ingresType,sqlType);
			if(colSize >= 0)
			{
				length = colSize;
			}
			
			Object defaultValue = defaultValueMap.get(dataRow.get("column_default_val"));
			boolean nullable = "Y".equals(dataRow.get("column_nulls"));
			
			boolean autoIncrement = false;
			if(column_bydefault_ident_index == -2)
			{
				// column_bydefault_ident available since Ingres 10
				column_bydefault_ident_index = dataRow.getVirtualTable().getColumnIndex(
						"column_bydefault_ident");
			}
			if(column_bydefault_ident_index != -1)
			{
				autoIncrement = "Y".equals(dataRow.get(column_bydefault_ident_index));
			}
			
			columns[i] = new ColumnMetaData(tableName,columnName,caption,type,length,scale,
					defaultValue,nullable,autoIncrement);
		}
		
		Map<IndexInfo, Set<String>> indexColumnMap = new LinkedHashMap<>();
		int rowCount = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE
			&& (flags & INDICES) != 0
		)
		{
			Set<String> pkColumns = new HashSet<>();
			
			if(primaryKeyMap != null)
			{
				List<VirtualTableRow> pkRows = primaryKeyMap.get(tableName);
				if(pkRows != null && !pkRows.isEmpty())
				{
					String indexName = (String)pkRows.get(0).get("constraint_name");
					for(VirtualTableRow pkRow : pkRows)
					{
						pkColumns.add((String)pkRow.get("column_name"));
					}
					indexColumnMap
						.put(new IndexInfo(indexName, IndexType.PRIMARY_KEY), pkColumns);
				}
			}
			
			if(indexMap != null)
			{
				List<VirtualTableRow> indexRows = indexMap.get(tableName);
				if(indexRows != null && !indexRows.isEmpty())
				{
					for(VirtualTableRow pkRow : indexRows)
					{
						String indexName = (String)pkRow.get("index_name");
						String columnName = (String)pkRow.get("column_name");
						if(indexName != null
							&& columnName != null
							&& !pkColumns.contains(columnName)
						)
						{
							boolean unique = "U".equals(pkRow.get("unique_rule"));
							IndexInfo info = new IndexInfo(indexName,unique ? IndexType.UNIQUE
								: IndexType.NORMAL);
							Set<String> columnNames = indexColumnMap.get(info);
							if(columnNames == null)
							{
								columnNames = new HashSet<>();
								indexColumnMap.put(info,columnNames);
							}
							columnNames.add(columnName);
						}
					}
				}
			}
			
		}
		
		Index[] indices = new Index[indexColumnMap.size()];
		int i = 0;
		for(IndexInfo indexInfo : indexColumnMap.keySet())
		{
			Set<String> columnList = indexColumnMap.get(indexInfo);
			String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name,indexInfo.type,indexColumns);
		}
		
		return new TableMetaData(table,columns,indices,rowCount);
	}
	
	
	/*
	 * Taken from JdbcDBMD
	 */
	private static int convToJavaType(int ingresType, int columnLength)
	{
		switch(ingresType)
		{
			case 22:
				return -1;
				
			case 23:
				return -2;
				
			case 24:
				return -3;
			
			case 25:
				return -4;
				
			case 27:
				return -9;
				
			case 26:
				return -15;
				
			case 28:
				return -16;
				
			// All return 1
			case 20:
			case 32:
				return 1;
				
			case 5:
			case 10:
				return 3;
			
			// All return 12
			case 21:
			case 33:
			case 34:
			case 37:
			case 41:
				return 12;
				
			case 38:
				return 16;
				
			case 4:
				return 91;
			
			// All return 92
			case 6:
			case 7:
			case 8:
				return 92;
				
			// All return 93
			case 3:
			case 9:
			case 18:
			case 19:
				return 93;
				
			case 30:
				switch(columnLength)
				{
					case 1:
						return -6;
					case 2:
						return 5;
					case 4:
						return 4;
					case 8:
						return -5;
					// All return 0
					case 3:
					case 5:
					case 6:
					case 7:
					default:
				}
				return 0;
				
			case 31:
				switch(columnLength)
				{
					case 4:
						return 7;
					case 8:
						return 8;
					default:
				}
				return 0;
			
			// All return 0
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 16:
			case 17:
			case 29:
			case 35:
			case 36:
			case 39:
			case 40:
			default:
		}
		return 0;
	}
	
	
	/*
	 * Taken from JdbcDBMD
	 */
	private static int colSize(int ingresType, int sqlType) throws SQLException
	{
		switch(sqlType)
		{
			case 3:
				if(ingresType == 5)
				{
					return 14;
				}
				return -1;
			
			// All return -1
			case -3:
			case -2:
			case 1:
			case 12:
				return -1;
				
			// All return 0
			case -4:
			case -1:
			case 16:
			case 2004:
			case 2005:
				return 0;
				
			case 91:
				return 10;
			case 92:
				return 8;
			case 93:
				return 29;
			case -6:
				return 4;
			case 5:
				return 6;
			case 4:
				return 11;
			case -5:
				return 20;
			case 7:
				return 7;
			case 8:
				return 15;
			// Returns -1
			default:
		}
		return -1;
	}
	
	
	@Override
	public EntityRelationshipModel getEntityRelationshipModel(ProgressMonitor monitor,
			TableInfo... tableInfos) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		monitor.setTaskName("");
		
		EntityRelationshipModel model = new EntityRelationshipModel();
		
		List<String> tables = new ArrayList<>();
		for(TableInfo table : tableInfos)
		{
			if(table.getType() == TableType.TABLE)
			{
				tables.add(table.getName());
			}
		}
		Collections.sort(tables);
		
		try(JDBCConnection jdbcConnection = (JDBCConnection)dataSource.openConnection())
		{
			String sbKeys = "select distinct p.schema_name, p.table_name, p.column_name, " +
					"f.schema_name, f.table_name, f.column_name, f.key_position, " +
					"f.constraint_name, p.constraint_name " +
				"from iikeys p, iiconstraints c, " +
					"iiref_constraints rc, iikeys f " +
				"where c.constraint_type = 'R' and c.constraint_name = rc.ref_constraint_name " +
					"and p.constraint_name = rc.unique_constraint_name " +
					"and f.constraint_name = rc.ref_constraint_name " +
					"and p.key_position = f.key_position and p.schema_name=? " +
				"order by 4, 5, 7";
			
			List<String> params = new ArrayList<>();
			params.add(dataSource.getUserName());
			
			try(Result rs = jdbcConnection.query(sbKeys, params.toArray()))
			{
				String pkTable = null;
				String fkTable = null;
				List<String> pkColumns = new ArrayList<>();
				List<String> fkColumns = new ArrayList<>();
				
				while(rs.next())
				{
					short keyPos = rs.getShort("key_position");
					
					if(keyPos == 1
						&& !pkColumns.isEmpty()
						&& tables.contains(pkTable)
						&& tables.contains(fkTable))
					{
						buildEntityRelationship(model, pkTable, fkTable, pkColumns, fkColumns);
					}
					
					pkTable = rs.getString(1).trim();
					fkTable = rs.getString(4).trim();
					
					pkColumns.add(rs.getString(2).trim());
					fkColumns.add(rs.getString(5).trim());
				}
				
				if(!pkColumns.isEmpty()
					&& tables.contains(pkTable)
					&& tables.contains(fkTable)
				)
				{
					buildEntityRelationship(model, pkTable, fkTable, pkColumns, fkColumns);
				}
			}
		}
		
		monitor.done();
		
		return model;
	}
	
	private static void buildEntityRelationship(
		EntityRelationshipModel model,
		String pkTable,
		String fkTable,
		List<String> pkColumns,
		List<String> fkColumns)
	{
		model.add(new EntityRelationship(
			new Entity(pkTable, pkColumns.toArray(new String[pkColumns.size()]), Cardinality.ONE),
			new Entity(fkTable, fkColumns.toArray(new String[fkColumns.size()]), Cardinality.MANY)));
		pkColumns.clear();
		fkColumns.clear();
	}
	
	private Map<String, List<VirtualTableRow>> toMap(VirtualTable vt, String columnName)
	{
		Map<String, List<VirtualTableRow>> columnMap = new HashMap<>();
		int tableNameColumnIndex = vt.getColumnIndex(columnName);
		for(VirtualTableRow row : vt.rows())
		{
			CollectionUtils.accumulate(columnMap,(String)row.get(tableNameColumnIndex),row);
		}
		return columnMap;
	}
	
	
	@Override
	public StoredProcedure[] getStoredProcedures(ProgressMonitor monitor) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<StoredProcedure> storedProcedures = new ArrayList<>();
		
		try
		{
			ConnectionProvider connectionProvider = dataSource.getConnectionProvider();
			
			try(Connection connection = connectionProvider.getConnection())
			{
				DatabaseMetaData meta = connection.getMetaData();
				
				// key: procedure name; value: list column maps, each column has
				// one own list entry
				Map<String, List<Map<String, Object>>> proceduresMap = new HashMap<>();
				Statement statement = connection.createStatement();
				addProcedureColumns(proceduresMap, statement);
				
				// catalog and schema only works with null
				// see #13485
				ResultSet rs = meta.getProcedures(null, null, null);
				addStoredProcedures(monitor, storedProcedures, proceduresMap, rs);
				
			}
		}
		catch(SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return storedProcedures.toArray(new StoredProcedure[storedProcedures.size()]);
	}
	
	
	private void addStoredProcedures(
		ProgressMonitor monitor,
		List<StoredProcedure> storedProcedures,
		Map<String, List<Map<String, Object>>> proceduresMap,
		ResultSet rs)
		throws SQLException
	{
		while(rs.next() && !monitor.isCanceled())
		{
			ReturnTypeFlavor returnTypeFlavor = ReturnTypeFlavor.VOID;
			String name = rs.getString("PROCEDURE_NAME");
			
			String description = rs.getString("REMARKS");
			
			DataType returnType = null;
			
			List<Param> params = new ArrayList<>();
			
			if(name != null && proceduresMap.containsKey(name))
			{
				for(Map<String, Object> map : proceduresMap.get(name))
				{
					
					if(map.containsValue("RESULT_TYPE"))
					{
						returnTypeFlavor = (ReturnTypeFlavor)map.get("RETURN_TYPE_FLAVOR");
						returnType = (DataType)map.get("DATA_TYPE");
					}
					else
					{
						addParamAccordingToType(params, map);
					}
				}
			}
			storedProcedures.add(new StoredProcedure(
				returnTypeFlavor,
				returnType,
				name,
				description,
				params.toArray(new Param[params.size()])
				));
		}
		rs.close();
	}
	
	/**
	 * Checks if the map if the key is one of the follow to add the according ParamType to the list
	 * <ol>
	 *     <li>IN</li>
	 *     <li>OUT</li>
	 *     <li>IN_OUT</li>
	 * </ol>
	 */
	private static void addParamAccordingToType(List<Param> params, Map<String, Object> map)
	{
		if(Boolean.TRUE.equals(map.get("IS_INPUT")))
		{
			params.add(new Param(ParamType.IN, (String)map.get("PARAM_NAME"), (DataType)map.get("DATA_TYPE")));
		}
		else if(Boolean.TRUE.equals(map.get("IS_OUTPUT")))
		{
			params.add(new Param(ParamType.OUT, (String)map.get("PARAM_NAME"), (DataType)map.get("DATA_TYPE")));
		}
		else if(Boolean.TRUE.equals(map.get("IS_INOUT")))
		{
			params.add(new Param(ParamType.IN_OUT, (String)map.get("PARAM_NAME"), (DataType)map.get("DATA_TYPE")));
		}
	}
	
	private void addProcedureColumns(Map<String, List<Map<String, Object>>> proceduresMap,
			Statement createStatement) throws SQLException, SQLSyntaxErrorException
	{
		String sqlProcedureName = null;
		ResultSet spColumnResultSet = null;
		try
		{
			sqlProcedureName = "procedure_name";
			spColumnResultSet = (JdbcRslt)createStatement
					.executeQuery("SELECT DISTINCT param_datatype_code,"
							+ sqlProcedureName
							+ ",param_input,param_output,param_inout, param_name,param_length FROM iiproc_params order by procedure_owner, procedure_name, param_sequence");
		}
		catch(SQLSyntaxErrorException e)
		{
			if(e.getSQLState().equals(4205))
			{
				// just in case the catalog iiproc_params does not exist try the
				// catalog iigwprocparams and use proc_name instead of
				// procedure_name.
				// Found in Ingres Jdbc driver but is never used there.
				sqlProcedureName = "proc_name";
				spColumnResultSet = (JdbcRslt)createStatement
						.executeQuery("SELECT DISTINCT param_datatype_code,"
								+ sqlProcedureName
								+ ",param_input,param_output,param_inout, param_name,param_length FROM iigwprocparams order by proc_owner, proc_name, param_sequence");
			}
			else
			{
				throw e;
			}
		}
		
		while(spColumnResultSet.next())
		{
			
			int javaParamType = convToJavaType(
					Math.abs(spColumnResultSet.getInt("param_datatype_code")),
					spColumnResultSet.getInt("param_length"));
			
			DataType dataType = DataType.get(javaParamType);
			String procedureName = spColumnResultSet.getString(sqlProcedureName).trim();
			
			String paramName = spColumnResultSet.getString("param_name").trim();
			boolean isInOut = false;
			boolean isOut = false;
			boolean isIn = false;
			
			if(spColumnResultSet.getString("param_input").equalsIgnoreCase("Y"))
			{
				isIn = true;
			}
			else if(spColumnResultSet.getString("param_output").equalsIgnoreCase("Y"))
			{
				isOut = true;
			}
			else if(spColumnResultSet.getString("param_inout").equalsIgnoreCase("Y"))
			{
				isInOut = true;
			}
			
			if(proceduresMap.get(procedureName) != null)
			{
				Map<String, Object> map = generateStringObjectMap(dataType, paramName, isInOut, isOut, isIn);
				
				proceduresMap.get(procedureName).add(map);
				
			}
			else
			{
				List<Map<String, Object>> list = new ArrayList<>();
				
				Map<String, Object> map = generateStringObjectMap(dataType, paramName, isInOut, isOut, isIn);
				
				list.add(map);
				
				proceduresMap.put(procedureName,list);
			}
		}
		
		spColumnResultSet.close();
		
		addReturnParameters(proceduresMap,createStatement);
	}
	
	private static Map<String, Object> generateStringObjectMap(
		DataType dataType,
		String paramName,
		boolean isInOut,
		boolean isOut,
		boolean isIn)
	{
		Map<String, Object> map = new HashMap<>();
		map.put("DATA_TYPE", dataType);
		map.put("IS_INPUT", isIn);
		map.put("IS_OUTPUT", isOut);
		map.put("IS_INOUT", isInOut);
		map.put("PARAM_NAME", paramName);
		return map;
	}
	
	private void addReturnParameters(Map<String, List<Map<String, Object>>> proceduresMap,
			Statement createStatement) throws SQLException
	{
		ResultSet resultTypeQuery = (JdbcRslt)createStatement
				.executeQuery("SELECT DISTINCT procedure_name,rescol_name,rescol_datatype_code,rescol_length FROM iiproc_rescols WHERE rescol_name like 'result_column%'");
		
		String lastProcedureName = "";
		
		Map<String, Map<String, Object>> tempMap = new HashMap<>();
		
		while(resultTypeQuery.next())
		{
			String procedure_name = resultTypeQuery.getString("procedure_name").trim();
			
			if(!lastProcedureName.equalsIgnoreCase(procedure_name))
			{
				
				int ifx_rescol_code = resultTypeQuery.getInt("rescol_datatype_code");
				int ifx_rescol_length = resultTypeQuery.getInt("rescol_length");
				
				int convToJavaType = convToJavaType(Math.abs(ifx_rescol_code),ifx_rescol_length);
				
				DataType dataType = DataType.get(convToJavaType);
				
				Map<String, Object> map = new HashMap<>();
				map.put("PARAM_NAME","RESULT_TYPE");
				map.put("DATA_TYPE",dataType);
				map.put("RETURN_TYPE_FLAVOR",ReturnTypeFlavor.TYPE);
				
				tempMap.put(procedure_name,map);
				lastProcedureName = procedure_name;
			}
			else
			{
				
				if(tempMap.get(procedure_name).containsValue(ReturnTypeFlavor.RESULT_SET))
				{
					break;
				}
				
				tempMap.get(procedure_name).put("RETURN_TYPE_FLAVOR",ReturnTypeFlavor.RESULT_SET);
				tempMap.get(procedure_name).put("DATA_TYPE",null);
				
			}
			
		}
		for(String key : tempMap.keySet())
		{
			if(proceduresMap.containsKey(key))
			{
				proceduresMap.get(key).add(tempMap.get(key));
			}
			else
			{
				List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
				list.add(tempMap.get(key));
				proceduresMap.put(key,list);
			}
		}
		resultTypeQuery.close();
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
	}
	
	
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
	}
	
	
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		return false;
	}
	
	
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
	}
	
	
	protected void appendEscapedName(String name, StringBuilder sb)
	{
	}
}

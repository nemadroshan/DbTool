package com.rn.dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.management.Query;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Repository;

import com.rn.ConnectionUtil;

import ch.qos.logback.classic.db.SQLBuilder;

@Repository
public class DatabaseDao {
	@Autowired
	private ConnectionUtil con;

	private Connection connection;

	public Boolean createConnection(String user, String pwd, String url) throws ClassNotFoundException, SQLException {
		connection = con.getConnection(user, pwd, url);
		System.out.println("connection :: " + connection);
		if (connection != null)
			return true;
		else
			return false;
	}

	public List<String> getAllTables() throws SQLException {
		final String query = "SELECT  TABLE_NAME FROM USER_TABLES";
		ResultSet rs = null;
		List<String> listOfTables = new ArrayList<String>();
		if (connection == null)
			throw new SQLException("Please Check DB Connection");
		PreparedStatement ps = connection.prepareStatement(query);
		rs = ps.executeQuery();
		if (rs != null) {
			while (rs.next()) {
				listOfTables.add(rs.getString(1));
			} // while
		} // if
		ps.close();
		return listOfTables;
	} // method

	public List<Map<String, Object>> getTableData(String tableName) throws SQLException {
		List<Map<String, Object>> listData = new ArrayList<Map<String, Object>>();
		String query = "SELECT * FROM " + tableName;
		if (connection == null)
			throw new SQLException("Please Check DB Connection");
		Statement st = connection.createStatement();
		ResultSet rs = st.executeQuery(query);

		ResultSetMetaData metaData = rs.getMetaData();
		int colCount = metaData.getColumnCount();

		while (rs.next()) {
			Map<String, Object> map = new HashMap<>();
			for (int i = 1; i <= colCount; i++) {
				map.put(metaData.getColumnName(i), rs.getObject(i));
			}
			listData.add(map);
			if (rs.isAfterLast()) {
				break;
			}
		}
		st.close();
		connection.close();
		return listData;
	}// method

	public Map<String, Object> executeAggrigateOperationalQuery(String query) throws SQLException {
		Map<String, Object> map = new HashMap<String, Object>();
		ResultSet rs = null;
		ResultSetMetaData metaData = null;
		int columnCounnt = 0;
		if (connection == null)
			throw new SQLException("Please Check DB Connection");
		Statement st = connection.createStatement();
		if (query != null)
			rs = st.executeQuery(query);
		if (rs != null)
			metaData = rs.getMetaData();
		columnCounnt = metaData.getColumnCount();
		if (rs.next()) {
			map.put(metaData.getColumnName(1), rs.getObject(1));
		}
		st.close();
		return map;
	}

	/*	public Map<String, Object> getTableColumnDetails(String tableName) throws SQLException {
			Map<String, Object> result = new HashMap<String, Object>();
			ResultSet rs = null;
			int count = 0;
			ResultSetMetaData metadata = null;
			tableName = tableName.toUpperCase();
			List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
			String query = "SELECT * FROM " + tableName;
			if(connection == null) throw new SQLException("Please Check DB Connection");
			Statement st = connection.createStatement();
			rs = st.executeQuery(query);
			if (rs != null) {
				metadata = rs.getMetaData();
				count = metadata.getColumnCount();
				for (int i = 1; i <= count; i++) {
					Map column = new LinkedHashMap<String, Object>();
					column.put(metadata.getColumnName(i), metadata.getColumnTypeName(i));
					column.put("isNullAble", metadata.isNullable(i) == 1 ? true : false);
					column.put("precision", metadata.getPrecision(i));
					column.put("scale", metadata.getScale(i));
					column.put("Size", metadata.getColumnDisplaySize(i));
					column.put("isAutoIncrement", metadata.isAutoIncrement(i));
					list.add(column);
				}
			}
			result.put("Table Name", tableName);
			result.put("Column Details", list);
			st.close();
			return result;
		}*/

	/*		public Map<String, Object> getTableColumnDetails(String tableName) throws SQLException {
				Map<String, Object> result = new HashMap<String, Object>();
				ResultSet rs = null;
				int count = 0;
				ResultSetMetaData metadata = null;
				tableName = tableName.toUpperCase();
				List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
				String query = "describe " + tableName+";";
				if(connection == null) throw new SQLException("Please Check DB Connection");
				Statement st = connection.createStatement();
				rs = st.executeQuery(query);
				if (rs != null) {
					metadata = rs.getMetaData();
					count = metadata.getColumnCount();		
					while(rs.next()) {
						Map map = new LinkedHashMap<String, Object>();
						map.put("Column Name", rs.getObject(1));
						map.put("Not Null", rs.getObject(2));
						map.put("Type", rs.getObject(3));
						list.add(map);
					}
				}
				result.put("Table Name", tableName);
				result.put("Column Details", list);
				System.out.println(list);
				st.close();
				return result;
			}*/

		public Map<String, Object> getTableColumnDetails(String tableName) throws SQLException {
			tableName = tableName.toUpperCase();
			Map result = new HashMap<String, Object>();
			String  query ="select * from USER_TAB_COLUMNS 	WHERE TABLE_NAME =? ORDER BY COLUMN_ID";
			PreparedStatement ps =null;
			ResultSet rs =null;
			List list = new ArrayList<>();
			ResultSetMetaData metadata = null;
			if(connection==null) throw new SQLException("Please Check the DB Connections");
			ps = connection.prepareStatement(query);
			ps.setString(1, tableName);
			rs =ps.executeQuery();
		    while(rs.next()) {
			   Map map = new LinkedHashMap<String,Object>();
			   map.put("tableName", rs.getObject("TABLE_NAME"));
			   map.put("columnName", rs.getObject("COLUMN_NAME"));
			   map.put("datatype", rs.getObject("DATA_TYPE"));
			   map.put("dataLength", rs.getObject("DATA_LENGTH"));
			   map.put("precision", rs.getObject("DATA_PRECISION"));
			   map.put("scale", rs.getObject("DATA_SCALE"));
			   map.put("nullable", rs.getObject("NULLABLE"));
			   map.put("columnId", rs.getObject("COLUMN_ID"));
			   list.add(map);
		   }
		   System.out.println(list);
		   result.put("columnDetails",list);
			return result;
		}

}// class

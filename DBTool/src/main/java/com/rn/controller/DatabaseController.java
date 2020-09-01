package com.rn.controller;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.rn.QueryBuilder;
import com.rn.dao.DatabaseDao;

@RestController
public class DatabaseController {
	@Autowired
	private DatabaseDao dao;

	@PostMapping(path = "/createConnection", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String setCon(@RequestBody String json) {
		Boolean flag = false;
		Map map = new LinkedHashMap<String, Object>();
		Object obj = JSONValue.parse(json);

		JSONObject jsonObject = (JSONObject) obj;
		if (jsonObject.containsKey("username") && jsonObject.containsKey("password") && jsonObject.containsKey("url")) {
			String user = jsonObject.get("username").toString();
			String pwd = jsonObject.get("password").toString();
			String url = jsonObject.get("url").toString();

			try {
				flag = dao.createConnection(user, pwd, url);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				map.put("Message", "Inavlid Username or password");
				map.put("Required", "username , password and url ");
				map.put("Error Code", 404);
				e.printStackTrace();
			}
			if (flag) {
				map.put("Message", "Connected");
				map.put("Status Code", 200);
				return JSONValue.toJSONString(map);
			} else {
				return JSONValue.toJSONString(map);
			}
		} else {
			map.put("Message", "Please enter the valid keys");
			map.put("username", "required");
			map.put("password", "required");
			map.put("url", "required");
			return JSONValue.toJSONString(map);
		}
	}

	@GetMapping(path = "/getTables", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getAllTables() {
		List<String> allTables = null;
		Map map = new HashMap<String, Object>();
		try {
			allTables = dao.getAllTables();
			if (allTables != null) {
				map.put("tables", allTables);
				map.put("status", 200);
			} else {
				map.put("ErrorMsg", "No tables found");
				map.put("status", 400);
			}
		} catch (SQLException se) {
			map.put("Message", se.getMessage());
			map.put("status", 400);
			se.printStackTrace();
		}
		return JSONValue.toJSONString(map);
	}// method

	// get table data by tableName

	@GetMapping(path = "/getTable/{tableName}", produces = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String getTableData(@PathVariable("tableName") String tableName) {
		Map<String, Object> map = new HashMap<>();
		if (tableName != null) {
			int count = 0;
			List<Map<String, Object>> tableData = null;
			try {
				tableData = dao.getTableData(tableName);
				count = tableData.size();
				map.put(tableName, tableData);
				map.put("rows", count);
			} catch (SQLException e) {
				map.put("Error Message ", e.getMessage());
				map.put("status", 404);
				e.printStackTrace();
			}
		} else {
			map.put("Error Message ", "Table Name is Mandetory");
			map.put("status", 500);
		}
		return JSONValue.toJSONString(map);
	}

	@GetMapping(path = "/getAggregateVal/{tableName}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody String getAggregateFunctionValue(@PathVariable("tableName") String tableName,
			@RequestBody String input) {
		Map<String, Object> map = new HashMap<String, Object>();
		JSONObject json = (JSONObject) JSONValue.parse(input);
		QueryBuilder builder = new QueryBuilder();
		try {
			String query = builder.createCustomQuery(json, tableName);
			Map<String, Object> output = dao.executeAggrigateOperationalQuery(query);
			map.put("Output", output);
			map.put("satus", 200);
		} catch (IllegalArgumentException iae) {
			map.put("Message", iae.getMessage());
			map.put("status", 404);
			iae.printStackTrace();
		} catch (SQLException se) {
			map.put("Message", se.getMessage());
			map.put("status", 404);
			se.printStackTrace();
		} catch (Exception e) {
			map.put("Message", e.getMessage());
			map.put("status", 500);
			e.printStackTrace();
		}
		return JSONValue.toJSONString(map);
	}

	@PostMapping(path = "/tableInfo/{table}", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getTableColumnDetails(@PathVariable("table") String tableName) {
		Map response = new LinkedHashMap<>();
		try {
			Map<String, Object> tableColumnDetails = dao.getTableColumnDetails(tableName);
			response.put(tableName, tableColumnDetails);
			response.put("status", 200);
		} catch (SQLException se) {
			response.put("Message", se.getMessage());
			response.put("status", 404);
			se.printStackTrace();
		} catch (Exception e) {
			response.put("Message", "Please Check The Db Connection");
			response.put("status", 500);
			e.printStackTrace();
		}
		return JSONValue.toJSONString(response);
	}

	
	
	@PostMapping("/test/{tableName}")
	public void test(@PathVariable("tableName") String tableName) throws Exception {
		System.out.println("Inside Test Method");
		dao.getTableColumnDetails(tableName);
	}
	
}// CLASS

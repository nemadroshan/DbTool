package com.rn;

import java.text.DateFormatSymbols;
import java.text.ParseException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Component;

@Component
public class QueryBuilder {
	public static String dateConvertor(String input) {
		String[] split = input.split("-");
		String initial = split[0].toUpperCase();
		int year = Integer.parseInt(split[1]);
		String[] shortMonths = new DateFormatSymbols().getShortMonths();
		// System.out.println(Arrays.toString(shortMonths));
		int monthNo = 0;
		int count = 0;
		for (String s : shortMonths) {
			count++;
			if (initial.equalsIgnoreCase(s)) {
				monthNo = count;
			}
		}
		if (monthNo <= 9)
			return "0" + monthNo + "-" + year;
		else
			return +monthNo + "-" + year;
	}

	public static String giveCompleteDate(String input) {
		String[] split = input.split("-");
		String date = split[0];
		int year = Integer.parseInt(split[2]);
		if (year > 2000) {
			// year = Integer.parseInt(String.valueOf(year).substring(2));
			throw new IllegalArgumentException("Invalid date Format");
		}
		String s = split[1] + "-" + year;
		date = date + "-" + dateConvertor(s);
		System.out.println(date);
		return date;
	}

	public String createCustomQuery(JSONObject json, String tableName) throws IllegalArgumentException {
		// apending the select
		StringBuilder query = new StringBuilder("SELECT ");
		String column = null;
		String opr = null;
		JSONObject date = null;
		JSONArray conditionColumns = null;
		JSONArray conditionValues = null;

		if (json.containsKey("column_name") && json.containsKey("out_operation") && json.containsKey("date")) {
			column = json.get("column_name").toString();
			opr = json.get("out_operation").toString();
			date = (JSONObject) json.get("date");
		} else {
			throw new IllegalArgumentException("Unable to find key column_name or out_operation or date ");
		}
		// checking operation finctions
		if (opr.equalsIgnoreCase("SUM")) {
			// apending round function with selected group functions
			query.append("ROUND(SUM(" + column + "), 2) ");
		} else if (opr.equalsIgnoreCase("AVG")) {
			query.append("ROUND(AVG(" + column + "), 2) ");
		} else if (opr.equalsIgnoreCase("MIN")) {
			query.append("ROUND(MIN(" + column + "), 2) ");
		} else if (opr.equalsIgnoreCase("MAX")) {
			query.append("ROUND(MAX(" + column + "), 2) ");
		} else {
			throw new IllegalArgumentException("Unable Perform Operation other than SUM,AVG,MIN,MAX");
		}

		// apending the table name
		query.append("FROM " + tableName);
		// apending where clause
		query.append(" WHERE (1=1)");

		if (json.containsKey("condition_on_column_names") && json.containsKey("condition_on_column_values")) {
			conditionColumns = (JSONArray) json.get("condition_on_column_names");
			conditionValues = (JSONArray) json.get("condition_on_column_values");
			if (conditionColumns.size() == conditionValues.size()) {
				for (int i = 0; i < conditionColumns.size(); i++) {
					query.append(" AND " + conditionColumns.get(i).toString() + " = '"
							+ conditionValues.get(i).toString() + "'");
				}
			}
		} // apending conditions from condition array
		else {
			throw new IllegalArgumentException(
					"Unable to find key 'condition_on_column_names' or 'condition_on_column_values' ");
		}
		// date condtions
		if (date.containsKey("type")) {
			if (date.get("type").toString().equalsIgnoreCase("MONTH")) {
				// month
				String col = date.get("column_name").toString();
				String s = date.get("value").toString();
				String value = dateConvertor(s);
				query.append(" AND TO_CHAR(TO_DATE(" + col + ",'dd-MM-YYYY HH:mi:ss'),'MM')");
				query.append(" BETWEEN");
				query.append(" TO_CHAR(TO_DATE('" + value + "', 'MM-YY'), 'MM')");
				query.append(" AND");
				query.append(" TO_CHAR(TO_DATE('" + value + "', 'MM-YY'), 'MM')");
			}

			else if (date.get("type").toString().equalsIgnoreCase("YEAR")) {
				// year
				try {
					String col = date.get("column_name").toString();
					Long input = (Long) date.get("value");
					String value = null;
					value = "01-01-" + input;
					query.append(" AND");
					query.append(" TO_CHAR(TO_DATE(" + col + ",'dd-MM-rrrr'),'rrrr') = TO_CHAR(TO_DATE('" + value
							+ "' ,'dd-MM-rrrr'),'rrrr')");
				} catch (ClassCastException e) {
					throw new IllegalArgumentException(" Year Value Must be Number format Ex 2019 ");
				}
			} else if (date.get("type").toString().equalsIgnoreCase("QUARTER")) {
				String value = date.get("value").toString();
				String[] split = value.split("-");
				String quaterType = split[0];
				String col = date.get("column_name").toString();
				int year = 2000 + Integer.parseInt(split[1]);
				String date1 = "";
				String date2 = "";
				if (quaterType.equalsIgnoreCase("Q1")) {
					date1 = "01-01-" + year;
					date2 = "31-03-" + year;
				} // inner if
				else if (quaterType.equalsIgnoreCase("Q2")) {
					date1 = "01-04-" + year;
					date2 = "30-06-" + year;
				} // inner else if
				else if (quaterType.equalsIgnoreCase("Q3")) {
					date1 = "01-07-" + year;
					date2 = "30-09-" + year;
				} // inner else if
				else if (quaterType.equalsIgnoreCase("Q4")) {
					date1 = "01-10-" + year;
					date2 = "31-12-" + year;
				} // innerelse if
				query.append(" AND");
				query.append(" TO_DATE(" + col + ",'dd-MM-rrrr')");
				query.append(" BETWEEN");
				query.append(" TO_DATE('" + date1 + "','dd-MM-rrrr')");
				query.append(" AND");
				query.append(" TO_DATE('" + date2 + "','dd-MM-rrrr')");
			} // else if quater
			else if (date.get("type").toString().equalsIgnoreCase("DATE")) {
				
				String col = date.get("column_name").toString();
				String input = date.get("value").toString();
				String value = giveCompleteDate(input);
				query.append(" AND");
				query.append(" TO_DATE(" + col + ",'dd-MM-YYYY') = TO_DATE('" + value + "','dd-MM-YYYY')");
			} // date else if closed
			else {
				throw new IllegalArgumentException(" invalid type value ");
			} // main else
		} else {
			throw new IllegalArgumentException(" 'type' key required ");
		}
		System.out.println(query.toString());

		return query.toString();
	}// method
}// class

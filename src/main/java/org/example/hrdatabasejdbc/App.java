package org.example.hrdatabasejdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.example.hrdatabasejdbc.model.Employee;

/**
 * Hello my first JDBC app!
 *
 */
public class App {
	public static String DB_URL = "jdbc:mysql://localhost:3306/hr_group3";
	public static String USERNAME = "root";
	public static String PASSWORD = "admin";

	public static Connection connection;

	public static void main(String[] args) throws SQLException {
		
		List<String> s = new ArrayList<>();
		connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
		Statement statement = connection.createStatement();

		String sql = "SELECT * FROM employees";
		ResultSet result = statement.executeQuery(sql);

		while (result.next()) {
			int id = result.getInt("employee_id");
			String firstName = result.getString("first_name");
			String lastName = result.getString("last_name");

			// System.out.print("Employee Id: " + id);
			// System.out.print("FirstName: " + firstName);
			// System.out.println("LastName: " + lastName);
		}

		// print all employees first name, last name, salary and job title
		sql = "SELECT e.first_name, e.last_name, e.salary, j.job_title from employees e "
				+ "inner join jobs j on j.job_id = e.job_id";

		result = statement.executeQuery(sql);

		while (result.next()) {
			String firstName = result.getString("first_name");
			String lastName = result.getString("last_name");
			double salary = result.getDouble("salary");
			String jobTitle = result.getString("job_title");

			// System.out.print("FirstName: " + firstName);
			// System.out.print(" LastName: " + lastName);
			// System.out.print(" salary: " + salary);
			// System.out.println(" job title: " + jobTitle);
		}

		// updateEmployeeSalary();
		// getEmployeeById(103);

//		getEmployeeByIdWithPreparedStatement(100);
//		updateEmployeeSalaryWithPreparedStatement(10000, 100);
//		getEmployeeByIdWithPreparedStatement(100);

//		getEmployeesForSalaryRange(1000, 10000);

//		insertEmployee(new Employee(99, "dan", "boca", "danboca@", 
//				"0743", LocalDate.of(2010, 02, 02), "IT_PROG", 5000));
		
		Employee employee = getEmployeeByIdWithPreparedStatement(101);
		System.out.println(employee);
		
		updateJobForEmployeeAndInsertJobHistory("it_prog", 110, LocalDate.of(2018, 12, 14), LocalDate.of(2020, 12, 23));

		result.close();
		statement.close();
		connection.close();
	}

	public static void updateEmployeeSalary() throws SQLException {
		String sql = "UPDATE employees SET salary=15000 WHERE employee_id=103";
		Statement statement = connection.createStatement();
		statement.executeUpdate(sql);

		statement.close();
	}

	public static void updateEmployeeSalaryWithPreparedStatement(double salary, int employeeId) throws SQLException {
		String sql = "UPDATE employees SET salary=? WHERE employee_id=?";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.setDouble(1, salary);
		statement.setInt(2, employeeId);
		statement.executeUpdate();

		statement.close();
	}

	public static void getEmployeeById(int id) throws SQLException {
		String sql = "SELECT e.first_name, e.last_name, e.salary, j.job_title from employees e "
				+ "inner join jobs j on j.job_id = e.job_id where e.employee_id=103";
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(sql);

		while (result.next()) {
			String firstName = result.getString("first_name");
			String lastName = result.getString("last_name");
			double salary = result.getDouble("salary");
			String jobTitle = result.getString("job_title");

			System.out.print("FirstName: " + firstName);
			System.out.print(" LastName: " + lastName);
			System.out.print(" salary: " + salary);
			System.out.println(" job title: " + jobTitle);
		}

		result.close();
		statement.close();
	}

	public static Employee getEmployeeByIdWithPreparedStatement(int id) throws SQLException {
		String sql = "SELECT e.first_name, e.last_name, e.salary, j.job_id from employees e "
				+ "inner join jobs j on j.job_id = e.job_id where e.employee_id=?";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.setInt(1, id);
		ResultSet result = statement.executeQuery();

		Employee employee = new Employee();
		while (result.next()) {
			String firstName = result.getString("first_name");
			String lastName = result.getString("last_name");
			double salary = result.getDouble("salary");
			String jobId = result.getString("job_id");

			employee.setFirsName(firstName);
			employee.setLastName(lastName);
			employee.setSalary(salary);
			employee.setJobId(jobId);
		}
		
		result.close();
		statement.close();
		
		return employee;
	}

	public static void getEmployeesForSalaryRange(double minSalary, double maxSalary) throws SQLException {
		String sql = "Select e.first_name, e.last_name, e.salary from employees e" + " where salary between ? and ?";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.setDouble(1, minSalary);
		statement.setDouble(2, maxSalary);
		ResultSet result = statement.executeQuery();

		while (result.next()) {
			String firstName = result.getString("first_name");
			String lastName = result.getString("last_name");
			Double salary = result.getDouble("salary");

			System.out.print("First Name: " + firstName);
			System.out.print("Last Name: " + lastName);
			System.out.println("Salary is: " + salary);
		}

	}

	public static void insertEmployee(Employee employee) throws SQLException {

		String sql = "insert into employees(employee_id, first_name,last_name,email,phone_number,hire_date,job_id,salary) "
				+ "values (?,?,?,?,?,?,?,?)";

		PreparedStatement statement = connection.prepareStatement(sql);
		statement.setInt(1, employee.getId());
		statement.setString(2, employee.getFirsName());
		statement.setString(3, employee.getLastName());
		statement.setString(4, employee.getEmail());
		statement.setString(5, employee.getPhoneNumber());
		statement.setDate(6, Date.valueOf(employee.getHireDate()));
		statement.setString(7, employee.getJobId());
		statement.setDouble(8, employee.getSalary());

		statement.executeUpdate();
		statement.close();
	}
	
	public static void updateJobForEmployeeAndInsertJobHistory(String jobId, int employeeId, LocalDate startDate, LocalDate endDate){
		String sqlUpdateJob = "Update employees set job_id = ? where employee_id = ?";
		String sqlInsertJobHistory = "Insert into job_history (employee_id, start_date, end_date, job_id) "
		+ "values (?,?,?,?)";
		
		try {
			connection.setAutoCommit(false);
			
			PreparedStatement updateStatement = connection.prepareStatement(sqlUpdateJob);
			updateStatement.setString(1, jobId);
			updateStatement.setInt(2, employeeId);
			updateStatement.executeUpdate();
			updateStatement.close();
			
			PreparedStatement insertStatement = connection.prepareStatement(sqlInsertJobHistory);
			insertStatement.setInt(1, employeeId);
			insertStatement.setDate(2, Date.valueOf(startDate));
			insertStatement.setDate(3, Date.valueOf(endDate));
//			insertStatement.setNull(3, Types.DATE);
			insertStatement.setString(4, jobId);
			
			insertStatement.executeUpdate();
			insertStatement.close();
			connection.commit();
			
		} catch (SQLException e) {
			try {
				connection.rollback();
				connection.setAutoCommit(true);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		
		}
		
	}
	
	

}

using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Text;

namespace GenericSqlHelper
{
    public class GenericSqlHelper
    {
        #region Execute DataSet
        /// <summary>
        /// Execute query and return dataset
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="commandType"></param>
        /// <param name="paramaters"></param>
        /// <returns></returns>
        public static DataSet ExecuteDataSet(DbContext context, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            DataSet dtSet = new();
            SqlConnection sqlConnection = (SqlConnection)context.Database.GetDbConnection();
            if (sqlConnection.State == ConnectionState.Closed)
            {
                sqlConnection.Open();
            }
            using (SqlCommand command = new(query, sqlConnection))
            {
                SqlDataAdapter adapter = new(command);
                command.CommandType = commandType;
                if (paramaters != null)
                {
                    foreach (var para in paramaters)
                    {
                        command.Parameters.Add(para);
                    }
                }
                adapter.Fill(dtSet);
            }
            return dtSet;
        }

        /// <summary>
        /// Execute query and return dataset
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="commandType"></param>
        /// <param name="paramaters"></param>
        /// <returns></returns>
        public static DataSet ExecuteDataSet(string connectionString, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            using SqlConnection sqlConnection = new(connectionString);
            DataSet dtSet = new();
            if (sqlConnection.State == ConnectionState.Closed)
            {
                sqlConnection.Open();
            }
            using (SqlCommand command = new(query, sqlConnection))
            {
                SqlDataAdapter adapter = new(command);

                command.CommandType = commandType;
                if (paramaters != null)
                {
                    foreach (var para in paramaters)
                    {
                        command.Parameters.Add(para);
                    }
                }
                adapter.Fill(dtSet);
            }
            return dtSet;
        }
        #endregion


        #region Execute Scalar
        private static object ExecuteScalar(SqlConnection sqlConnection, string query, IEnumerable<object> paramaters, CommandType commandType)
        {
            if (sqlConnection.State == ConnectionState.Closed)
            {
                sqlConnection.Open();
            }

            using SqlCommand command = new(query, sqlConnection);
            command.CommandType = commandType;
            if (paramaters != null)
            {
                foreach (var para in paramaters)
                {
                    command.Parameters.Add(para);
                }
            }
            return command.ExecuteScalar();
        }

        /// <summary>
        /// Execute query and return data of first column and first row.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="query"></param>
        /// <param name="paramaters"></param>
        /// <param name="commandType"></param>
        /// <returns></returns>
        public static object ExecuteScalar(DbContext context, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            SqlConnection sqlConnection = (SqlConnection)context.Database.GetDbConnection();
            return ExecuteScalar(sqlConnection, query, paramaters, commandType);
        }

        /// <summary>
        /// Execute query and return data of first column and first row.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="query"></param>
        /// <param name="paramaters"></param>
        /// <param name="commandType"></param>
        /// <returns></returns>
        public static object ExecuteScalar(string connectionString, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            using SqlConnection sqlConnection = new(connectionString);
            return ExecuteScalar(sqlConnection, query, paramaters, commandType);
        }

        /// <summary>
        /// Execute query and return data of first column and first row. Convert into generic value type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="query"></param>
        /// <param name="paramaters"></param>
        /// <param name="commandType"></param>
        /// <returns></returns>
        public static T ExecuteScalar<T>(DbContext context, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure) where T : struct
        {
            SqlConnection sqlConnection = (SqlConnection)context.Database.GetDbConnection();
            var result = ExecuteScalar(sqlConnection, query, paramaters, commandType);
            return (T)result;
        }

        /// <summary>
        /// Execute query and return data of first column and first row. Convert into generic value type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="query"></param>
        /// <param name="paramaters"></param>
        /// <param name="commandType"></param>
        /// <returns></returns>
        public static T ExecuteScalar<T>(string connectionString, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure) where T : struct
        {
            using SqlConnection sqlConnection = new(connectionString);
            var result = ExecuteScalar(sqlConnection, query, paramaters, commandType);
            return (T)result;
        }
        #endregion


        #region Execute Command Non Query
        /// <summary>
        /// exec nonquery using for insert, update, delete
        /// </summary>
        /// <typeparam name="T">db context</typeparam>
        /// <param name="sqlCommand"> </param>
        /// <param name="paramaters"></param>
        /// <returns></returns>
        public static int ExecuteCommand(DbContext context, string sqlCommand, IEnumerable<object> paramaters = null)
        {
            SqlConnection sqlConnection = (SqlConnection)context.Database.GetDbConnection();
            SqlCommand command = new(sqlCommand, sqlConnection);
            if (sqlConnection.State == ConnectionState.Closed)
            {
                sqlConnection.Open();
            }
            using (command)
            {
                command.CommandType = CommandType.Text;
                if (paramaters != null)
                {
                    foreach (var para in paramaters)
                    {
                        command.Parameters.Add(para);
                    }
                }
                var result = command.ExecuteNonQuery();
                return result;
            }
        }

        /// <summary>
        /// exec nonquery using for insert, update, delete
        /// </summary>
        /// <typeparam name="T">db context</typeparam>
        /// <param name="sqlCommand"> </param>
        /// <param name="paramaters"></param>
        /// <returns></returns>
        public static int ExecuteCommand(string connectionString, string sqlCommand, IEnumerable<object> paramaters = null)
        {
            using SqlConnection sqlConnection = new(connectionString);
            SqlCommand command = new(sqlCommand, sqlConnection);
            if (sqlConnection.State == ConnectionState.Closed)
            {
                sqlConnection.Open();
            }
            using (command)
            {
                command.CommandType = CommandType.Text;
                if (paramaters != null)
                {
                    foreach (var para in paramaters)
                    {
                        command.Parameters.Add(para);
                    }
                }
                var result = command.ExecuteNonQuery();
                return result;
            }
        }
        #endregion


        #region Execute DataTable
        /// <summary>
        /// Execute query return datatable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="commandType"></param>
        /// <param name="paramaters"></param>
        /// <returns></returns>
        public static DataTable ExecuteDataTable(DbContext context, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            var dataSet = ExecuteDataSet(context, query, paramaters, commandType);
            return dataSet.Tables[0];
        }

        /// <summary>
        /// Execute query return datatable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="commandType"></param>
        /// <param name="paramaters"></param>
        /// <returns></returns>
        public static DataTable ExecuteDataTable(string connectionString, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure)
        {
            var dataSet = ExecuteDataSet(connectionString, query, paramaters, commandType);
            return dataSet.Tables[0];
        }
        #endregion


        #region Execute Entity
        /// <summary>
        /// Exec query return entity
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="commandType"></param>
        /// <param name="paramaters"></param>
        /// <returns></returns>
        public static IList<T> ExecuteEntity<T>(DbContext context, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure)
            where T : new()
        {
            DataTable tb = ExecuteDataTable(context, query, paramaters, commandType);
            return tb.ToList<T>();
        }
        /// <summary>
        /// Exec query return entity
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="commandType"></param>
        /// <param name="paramaters"></param>
        /// <returns></returns>
        public static IList<T> ExecuteEntity<T>(string connectionString, string query, IEnumerable<object> paramaters = null, CommandType commandType = CommandType.StoredProcedure)
            where T : new()
        {
            DataTable tb = ExecuteDataTable(connectionString, query, paramaters, commandType);
            return tb.ToList<T>();
        }
        #endregion


        #region Execute Add List

        #endregion
        /// <summary>
        /// Insert list data 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="TableName"></param>
        /// <param name="listData"></param>
        public static void ExecAddList<T>(DbContext context, string TableName, IEnumerable<T> listData)
        {
            SqlConnection connection = (SqlConnection)context.Database.GetDbConnection();
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }
            SqlTransaction transaction = connection.BeginTransaction();

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BatchSize = 100;
                bulkCopy.DestinationTableName = "dbo." + TableName;
                try
                {
                    bulkCopy.WriteToServer(listData.ToDataTable());
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    connection.Close();
                }
            }

            transaction.Commit();
        }

        /// <summary>
        /// Insert list data 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionString"></param>
        /// <param name="TableName"></param>
        /// <param name="listData"></param>
        public static void ExecAddList<T>(string connectionString, string TableName, IEnumerable<T> listData)
        {
            using var connection = new SqlConnection(connectionString);
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }
            SqlTransaction transaction = connection.BeginTransaction();

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BatchSize = 100;
                bulkCopy.DestinationTableName = "dbo." + TableName;
                try
                {
                    bulkCopy.WriteToServer(listData.ToDataTable());
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    connection.Close();
                }
            }

            transaction.Commit();
        }

        /// <summary>
        /// Insert data table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connect"></param>
        /// <param name="TableName"></param>
        /// <param name="lstItem"></param>
        public static void ExecAddListDataTable(DbContext context, string TableName, DataTable dt)
        {
            SqlConnection connection = (SqlConnection)context.Database.GetDbConnection();

            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }
            SqlTransaction transaction = connection.BeginTransaction();

            using (var bulkCopy = new SqlBulkCopy(connection, (SqlBulkCopyOptions.KeepNulls & SqlBulkCopyOptions.KeepIdentity), transaction))
            {
                bulkCopy.BatchSize = 100;
                bulkCopy.DestinationTableName = TableName;
                try
                {
                    bulkCopy.WriteToServer(dt);
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    connection.Close();
                }
            }

            transaction.Commit();
        }

        /// <summary>
        /// Insert data table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connect"></param>
        /// <param name="TableName"></param>
        /// <param name="lstItem"></param>
        public static void ExecuteAddDataTable(string connectionString, string TableName, DataTable dt)
        {
            using SqlConnection connection = new(connectionString);
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            SqlTransaction transaction = connection.BeginTransaction();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, (SqlBulkCopyOptions.KeepNulls & SqlBulkCopyOptions.KeepIdentity), transaction))
            {
                bulkCopy.BatchSize = 100;
                bulkCopy.DestinationTableName = TableName;
                try
                {
                    bulkCopy.WriteToServer(dt);
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    connection.Close();
                }
            }

            transaction.Commit();
        }
    }
}

# server.py
from mcp.server.fastmcp import FastMCP
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
from typing import List, Dict, Any, Optional
import os

# Create an MCP server
mcp = FastMCP("Excel Data Manager")
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
# ----- Excel Operations -----

@mcp.tool()
def read_excel(filename: str, sheet_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Read an Excel file and return its data
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to read (None for all sheets)
        
    Returns:
        Dictionary with sheet names and their data
    """
    try:
        filename = os.path.join(BASE_DIR, filename)
        if sheet_name:
            df = pd.read_excel(filename, sheet_name=sheet_name)
            return {
                "success": True,
                "data": {sheet_name: df.to_dict(orient="records")},
                "columns": {sheet_name: df.columns.tolist()},
                "shape": {sheet_name: df.shape}
            }
        else:
            excel_data = pd.read_excel(filename, sheet_name=None)
            result = {"success": True, "data": {}, "columns": {}, "shape": {}}
            for sheet, df in excel_data.items():
                result["data"][sheet] = df.to_dict(orient="records")
                result["columns"][sheet] = df.columns.tolist()
                result["shape"][sheet] = df.shape
            return result
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def write_excel(filename: str, data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Write data to an Excel file
    
    Args:
        filename: Name of the Excel file
        data: Dictionary with sheet names and their data
        
    Returns:
        Status of the operation
    """
    try:
        with pd.ExcelWriter(filename) as writer:
            for sheet_name, sheet_data in data.items():
                df = pd.DataFrame(sheet_data)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        return {"success": True, "message": f"Data written to {filename} successfully"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def get_excel_sheets(filename: str) -> Dict[str, Any]:
    """
    Get the list of sheets in an Excel file
    
    Args:
        filename: Name of the Excel file
        
    Returns:
        List of sheet names
    """
    try:
        xls = pd.ExcelFile(filename)
        return {"success": True, "sheets": xls.sheet_names}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ----- Data Filtering -----

@mcp.tool()
def filter_data(filename: str, sheet_name: str, filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter Excel data based on conditions
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to filter
        filters: Dictionary of column:value pairs for filtering
        
    Returns:
        Filtered data
    """
    try:
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        # Apply each filter
        for column, value in filters.items():
            if column in df.columns:
                if isinstance(value, list):
                    df = df[df[column].isin(value)]
                else:
                    df = df[df[column] == value]
        
        return {
            "success": True,
            "data": df.to_dict(orient="records"),
            "count": len(df),
            "columns": df.columns.tolist()
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def search_data(filename: str, sheet_name: str, search_term: str, columns: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Search for a term in Excel data
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to search
        search_term: Term to search for
        columns: List of columns to search in (None for all columns)
        
    Returns:
        Matching data
    """
    try:
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        # Convert all columns to string for searching
        df_str = df.astype(str)
        
        # Create mask for matching rows
        if columns:
            # Only search in specified columns
            mask = pd.Series(False, index=df.index)
            for col in columns:
                if col in df.columns:
                    mask = mask | df_str[col].str.contains(search_term, case=False, na=False)
        else:
            # Search in all columns
            mask = pd.Series(False, index=df.index)
            for col in df.columns:
                mask = mask | df_str[col].str.contains(search_term, case=False, na=False)
        
        result_df = df[mask]
        
        return {
            "success": True,
            "data": result_df.to_dict(orient="records"),
            "count": len(result_df),
            "columns": result_df.columns.tolist()
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ----- Data Analysis -----

@mcp.tool()
def summarize_data(filename: str, sheet_name: str, columns: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Get statistical summary of Excel data
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to summarize
        columns: List of columns to summarize (None for all numeric columns)
        
    Returns:
        Statistical summary
    """
    try:
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        # Filter columns if specified
        if columns:
            df = df[columns]
        
        # Get basic statistics for numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        stats = {}
        if numeric_cols:
            stats = df[numeric_cols].describe().to_dict()
        
        # Get value counts for categorical columns
        categorical_cols = df.select_dtypes(exclude=['number']).columns.tolist()
        category_counts = {}
        for col in categorical_cols:
            category_counts[col] = df[col].value_counts().to_dict()
        
        return {
            "success": True,
            "statistics": stats,
            "category_counts": category_counts,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": df.columns.tolist()
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ----- Data Visualization -----

@mcp.tool()
def visualize_chart(
    filename: str, 
    sheet_name: str, 
    chart_type: str,
    x_column: str,
    y_columns: List[str],
    title: str = "Chart",
    figsize: List[int] = [10, 6]
) -> Dict[str, Any]:
    """
    Create a visualization of Excel data
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to visualize
        chart_type: Type of chart (bar, line, scatter, pie, hist)
        x_column: Column for x-axis
        y_columns: Columns for y-axis
        title: Chart title
        figsize: Figure size [width, height]
        
    Returns:
        Base64 encoded image of the chart
    """
    try:
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        # Create figure
        plt.figure(figsize=(figsize[0], figsize[1]))
        
        if chart_type.lower() == "bar":
            df.plot(x=x_column, y=y_columns, kind='bar', title=title)
        elif chart_type.lower() == "line":
            df.plot(x=x_column, y=y_columns, kind='line', title=title)
        elif chart_type.lower() == "scatter":
            if len(y_columns) == 1:
                df.plot(x=x_column, y=y_columns[0], kind='scatter', title=title)
            else:
                return {"success": False, "error": "Scatter plot requires exactly one y column"}
        elif chart_type.lower() == "pie" and len(y_columns) == 1:
            # For pie charts, we need series data
            df.plot(kind='pie', y=y_columns[0], labels=df[x_column], title=title)
        elif chart_type.lower() == "hist":
            df[y_columns].plot(kind='hist', title=title)
        else:
            return {"success": False, "error": f"Unsupported chart type: {chart_type}"}
        
        plt.tight_layout()
        
        # Save plot to a bytes buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        
        # Encode the bytes as base64
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close()
        
        return {
            "success": True,
            "image": img_base64,
            "chart_type": chart_type,
            "title": title
        }
    except Exception as e:
        plt.close()
        return {"success": False, "error": str(e)}

@mcp.tool()
def create_pivot_table(
    filename: str,
    sheet_name: str,
    index: List[str],
    values: List[str],
    columns: Optional[List[str]] = None,
    aggfunc: str = "mean"
) -> Dict[str, Any]:
    """
    Create a pivot table from Excel data
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to use
        index: Columns to use as index
        values: Columns to aggregate
        columns: Columns to use as columns
        aggfunc: Aggregation function (mean, sum, count)
        
    Returns:
        Pivot table data
    """
    try:
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        # Map string aggfunc to actual function
        agg_map = {
            "mean": "mean",
            "sum": "sum",
            "count": "count",
            "min": "min",
            "max": "max"
        }
        
        # Use the specified aggfunc if valid, else default to mean
        agg = agg_map.get(aggfunc.lower(), "mean")
        
        # Create pivot table
        pivot = pd.pivot_table(
            df, 
            values=values,
            index=index,
            columns=columns,
            aggfunc=agg
        )
        
        # Convert pivot table to dict for JSON serialization
        pivot_dict = pivot.reset_index().to_dict(orient="records")
        
        return {
            "success": True,
            "pivot_data": pivot_dict,
            "index": index,
            "values": values,
            "columns": columns,
            "aggfunc": agg
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ----- Update Operations -----

@mcp.tool()
def update_cell(filename: str, sheet_name: str, row_identifier: Dict[str, Any], column: str, new_value: Any) -> Dict[str, Any]:
    """
    Update a specific cell in an Excel file
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to update
        row_identifier: Dictionary to identify the row (column:value)
        column: Column to update
        new_value: New value for the cell
        
    Returns:
        Status of the operation
    """
    try:
        # Read the excel file
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        # Create a mask to identify the row
        mask = pd.Series(True, index=df.index)
        for col, val in row_identifier.items():
            mask = mask & (df[col] == val)
        
        # Check if we found the row
        if not mask.any():
            return {"success": False, "error": "No matching row found"}
        
        # Update the cell
        row_idx = mask.idxmax()
        old_value = df.at[row_idx, column]
        df.at[row_idx, column] = new_value
        
        # Write back to Excel
        with pd.ExcelWriter(filename) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        return {
            "success": True,
            "message": f"Updated cell at column '{column}' for row identified by {row_identifier}",
            "old_value": str(old_value),
            "new_value": str(new_value)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def add_row(filename: str, sheet_name: str, row_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add a new row to an Excel file
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to update
        row_data: Dictionary with column:value pairs for the new row
        
    Returns:
        Status of the operation
    """
    try:
        # Read the excel file
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        # Validate that all columns exist
        missing_cols = [col for col in row_data.keys() if col not in df.columns]
        if missing_cols:
            return {"success": False, "error": f"Columns not found: {missing_cols}"}
        
        # Add the new row
        df = df.append(row_data, ignore_index=True)
        
        # Write back to Excel
        with pd.ExcelWriter(filename) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        return {
            "success": True,
            "message": f"Added new row with data: {row_data}",
            "new_row_index": len(df) - 1
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def delete_rows(filename: str, sheet_name: str, filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Delete rows from an Excel file based on filters
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to update
        filters: Dictionary with column:value pairs for filtering rows to delete
        
    Returns:
        Status of the operation
    """
    try:
        # Read the excel file
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        original_row_count = len(df)
        
        # Create mask for rows to keep
        mask = pd.Series(True, index=df.index)
        for col, val in filters.items():
            if col in df.columns:
                if isinstance(val, list):
                    mask = mask & (~df[col].isin(val))
                else:
                    mask = mask & (df[col] != val)
        
        # Apply the mask to keep rows
        df = df[mask]
        
        # Write back to Excel
        with pd.ExcelWriter(filename) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        rows_deleted = original_row_count - len(df)
        
        return {
            "success": True,
            "message": f"Deleted {rows_deleted} rows matching filters: {filters}",
            "rows_deleted": rows_deleted,
            "rows_remaining": len(df)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ----- Creative Features -----

@mcp.tool()
def detect_anomalies(filename: str, sheet_name: str, columns: List[str], threshold: float = 2.0) -> Dict[str, Any]:
    """
    Detect anomalies in Excel data using Z-score
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to analyze
        columns: Numeric columns to check for anomalies
        threshold: Z-score threshold (default: 2.0)
        
    Returns:
        Rows with anomalies
    """
    try:
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        # Check that all columns exist and are numeric
        for col in columns:
            if col not in df.columns:
                return {"success": False, "error": f"Column not found: {col}"}
            if not pd.api.types.is_numeric_dtype(df[col]):
                return {"success": False, "error": f"Column is not numeric: {col}"}
        
        # Calculate z-scores for each column
        anomalies = {}
        for col in columns:
            mean = df[col].mean()
            std = df[col].std()
            if std == 0:  # Avoid division by zero
                continue
                
            z_scores = (df[col] - mean) / std
            anomaly_rows = df[abs(z_scores) > threshold]
            
            if not anomaly_rows.empty:
                anomalies[col] = {
                    "z_score_threshold": threshold,
                    "anomaly_rows": anomaly_rows.to_dict(orient="records"),
                    "count": len(anomaly_rows)
                }
        
        return {
            "success": True,
            "anomalies": anomalies,
            "columns_checked": columns,
            "threshold": threshold
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def recommend_charts(filename: str, sheet_name: str) -> Dict[str, Any]:
    """
    Automatically recommend chart types based on data structure
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to analyze
        
    Returns:
        Chart recommendations
    """
    try:
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        recommendations = []
        
        # Get numeric and categorical columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(exclude=['number']).columns.tolist()
        
        # Time series recommendation
        date_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        for date_col in date_cols:
            for num_col in numeric_cols:
                recommendations.append({
                    "chart_type": "line",
                    "x_column": date_col,
                    "y_columns": [num_col],
                    "reason": f"Time series analysis of {num_col} over {date_col}"
                })
        
        # Categorical vs numeric recommendations
        for cat_col in categorical_cols:
            unique_cats = df[cat_col].nunique()
            if 2 <= unique_cats <= 10:  # Only recommend if we have a reasonable number of categories
                for num_col in numeric_cols:
                    recommendations.append({
                        "chart_type": "bar",
                        "x_column": cat_col,
                        "y_columns": [num_col],
                        "reason": f"Compare {num_col} across different {cat_col} categories"
                    })
                    
                    if unique_cats <= 7:  # Pie charts work best with few categories
                        recommendations.append({
                            "chart_type": "pie",
                            "x_column": cat_col,
                            "y_columns": [num_col],
                            "reason": f"Show proportion of {num_col} by {cat_col}"
                        })
        
        # Correlation between numeric columns
        if len(numeric_cols) >= 2:
            for i, col1 in enumerate(numeric_cols):
                for col2 in numeric_cols[i+1:]:
                    recommendations.append({
                        "chart_type": "scatter",
                        "x_column": col1,
                        "y_columns": [col2],
                        "reason": f"Examine correlation between {col1} and {col2}"
                    })
        
        # Distribution recommendations
        for num_col in numeric_cols:
            recommendations.append({
                "chart_type": "hist",
                "x_column": None,
                "y_columns": [num_col],
                "reason": f"View distribution of {num_col}"
            })
        
        return {
            "success": True,
            "recommendations": recommendations,
            "data_summary": {
                "row_count": len(df),
                "numeric_columns": numeric_cols,
                "categorical_columns": categorical_cols,
                "date_columns": date_cols
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def export_to_csv(filename: str, sheet_name: str, output_filename: Optional[str] = None) -> Dict[str, Any]:
    """
    Export Excel sheet to CSV
    
    Args:
        filename: Name of the Excel file
        sheet_name: Sheet to export
        output_filename: Name for the output CSV file (default: same as sheet name)
        
    Returns:
        Status of the operation
    """
    try:
        filename = os.path.join(BASE_DIR, filename)
        df = pd.read_excel(filename, sheet_name=sheet_name)
        
        if output_filename is None:
            # Create output filename based on input filename and sheet name
            base_name = os.path.splitext(filename)[0]
            output_filename = f"{base_name}_{sheet_name}.csv"
        
        df.to_csv(output_filename, index=False)
        
        return {
            "success": True,
            "message": f"Exported sheet '{sheet_name}' to '{output_filename}'",
            "rows": len(df),
            "columns": len(df.columns)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ----- REST Resources -----

@mcp.resource("excel://{filename}/sheets")
def get_excel_sheet_list(filename: str) -> Dict[str, Any]:
    """Get a list of sheets in an Excel file"""
    return get_excel_sheets(filename)

@mcp.resource("excel://{filename}/sheet/{sheet_name}")
def get_excel_sheet_data(filename: str, sheet_name: str) -> Dict[str, Any]:
    """Get data from a specific Excel sheet"""
    filename = os.path.join(BASE_DIR, filename)
    return read_excel(filename, sheet_name)

@mcp.resource("excel://{filename}/sheet/{sheet_name}/summary")
def get_excel_sheet_summary(filename: str, sheet_name: str) -> Dict[str, Any]:
    """Get summary of a specific Excel sheet"""
    return summarize_data(filename, sheet_name)

@mcp.resource("excel://{filename}/sheet/{sheet_name}/filter")
def filter_excel_sheet_data(filename: str, sheet_name: str) -> Dict[str, Any]:
    """Filter data in an Excel sheet"""
    # The request data is automatically provided by FastAPI
    # We can access it through the request object
    from fastapi import Request
    request = Request.get_current()
    body = request.json()
    filters = body.get("filters", {})
    return filter_data(filename, sheet_name, filters)

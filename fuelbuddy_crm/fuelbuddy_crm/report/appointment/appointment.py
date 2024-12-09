import frappe

def execute(filters=None):
    # Query to fetch the data
    query = """
    WITH ranked_data AS (
	    SELECT 
		business_head,
		posting_date,
		name,
		ROW_NUMBER() OVER (PARTITION BY business_head ORDER BY posting_date DESC) AS rank
	    FROM `tabSales Target`
	),
	filtered_sales_person AS (
	    SELECT 
		name AS team_user_name,
		custom_user, 
		custom_team
	    FROM `tabSales Person`
	    WHERE name NOT IN (
		'Abhishek Anand', 'Ankur Goel', 'Narayan Sathe', 
		'Nora Bali', 'Palash Gupta', 'Ravi', 
		'SK Arora', 'Setu Joshi', 'Vadivel Nandhyalam'
	    )
	),
	todo_data AS (
	    SELECT 
		td.owner,
		td.name AS Todo_ID,
		td.reference_type,
		td.reference_name,
		CASE 
		    WHEN td.date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN 
		        CASE 
		            WHEN td.reference_name = tc.name THEN tc.customer_name
		            WHEN td.reference_name = tl.name THEN tl.company_name
		            WHEN td.reference_name = tpo.name THEN tpo.customer_name
		            WHEN td.reference_name = ep.parent THEN 
		                CASE 
		                    WHEN ep.reference_docname = tc.name THEN tc.customer_name
		                    WHEN ep.reference_docname = tl.name THEN tl.company_name
		                    WHEN ep.reference_docname = tpo.name THEN tpo.customer_name
		                    ELSE ep.reference_docname
		                END
		            ELSE td.reference_name
		        END
		    ELSE '-' 
		END AS Yesterday,
		CASE 
		    WHEN td.date = CURRENT_DATE() THEN 
		        CASE 
		            WHEN td.reference_name = tc.name THEN tc.customer_name
		            WHEN td.reference_name = tl.name THEN tl.company_name
		            WHEN td.reference_name = tpo.name THEN tpo.customer_name
		            WHEN td.reference_name = ep.parent THEN 
		                CASE 
		                    WHEN ep.reference_docname = tc.name THEN tc.customer_name
		                    WHEN ep.reference_docname = tl.name THEN tl.company_name
		                    WHEN ep.reference_docname = tpo.name THEN tpo.customer_name
		                    ELSE ep.reference_docname
		                END
		            ELSE td.reference_name
		        END
		    ELSE '-' 
		END AS Today,
		CASE 
		    WHEN td.date = DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) THEN 
		        CASE 
		            WHEN td.reference_name = tc.name THEN tc.customer_name
		            WHEN td.reference_name = tl.name THEN tl.company_name
		            WHEN td.reference_name = tpo.name THEN tpo.customer_name
		            WHEN td.reference_name = ep.parent THEN 
		                CASE 
		                    WHEN ep.reference_docname = tc.name THEN tc.customer_name
		                    WHEN ep.reference_docname = tl.name THEN tl.company_name
		                    WHEN ep.reference_docname = tpo.name THEN tpo.customer_name
		                    ELSE ep.reference_docname
		                END
		            ELSE td.reference_name
		        END
		    ELSE '-' 
		END AS Tomorrow,
		CASE 
		    WHEN EXTRACT(WEEK FROM td.date) = EXTRACT(WEEK FROM CURRENT_DATE()) 
		      AND EXTRACT(YEAR FROM td.date) = EXTRACT(YEAR FROM CURRENT_DATE()) THEN 
		        CASE 
		            WHEN td.reference_name = tc.name THEN tc.customer_name
		            WHEN td.reference_name = tl.name THEN tl.company_name
		            WHEN td.reference_name = tpo.name THEN tpo.customer_name
		            WHEN td.reference_name = ep.parent THEN 
		                CASE 
		                    WHEN ep.reference_docname = tc.name THEN tc.customer_name
		                    WHEN ep.reference_docname = tl.name THEN tl.company_name
		                    WHEN ep.reference_docname = tpo.name THEN tpo.customer_name
		                    ELSE ep.reference_docname
		                END
		            ELSE td.reference_name
		        END
		    ELSE '-' 
		END AS Week
	    FROM 
		`tabToDo` td
	    LEFT JOIN `tabEvent Participants` ep ON ep.parent = td.reference_name AND ep.idx = 1
	    LEFT JOIN `tabCustomer` tc ON tc.name = td.reference_name OR ep.reference_docname = tc.name
	    LEFT JOIN `tabLead` tl ON tl.name = td.reference_name OR ep.reference_docname = tl.name
	    LEFT JOIN `tabOpportunity` tpo ON tpo.name = td.reference_name OR ep.reference_docname = tpo.name
	    WHERE 
		td.status = 'Open'
		AND td.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
	),
	todo_status_count AS (
	    SELECT 
		td.owner,
		COUNT(*) AS Count_Status_Open
	    FROM `tabToDo` td
	    WHERE 
		td.status = 'Open'
		AND td.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
	    GROUP BY td.owner
	),
	final_todo_data AS (
	    SELECT 
		td.Todo_ID,
		td.reference_name,
		td.reference_type,
		td.owner,
		td.Yesterday,
		td.Today,
		td.Tomorrow,
		td.Week,
		COALESCE(tsc.Count_Status_Open, 0) AS Count_Status_Open
	    FROM todo_data td
	    LEFT JOIN todo_status_count tsc ON tsc.owner = td.owner
	    WHERE 
		td.Yesterday != '-' OR td.Today != '-' OR td.Tomorrow != '-' OR td.Week != '-'
	)

	SELECT DISTINCT
	    sp.custom_team AS Team,
	    sp.team_user_name AS BD,
	    bd.zonal_head_sales_person AS Zonal_head,
	    todo.Todo_ID,
	    CASE 
		WHEN todo.reference_type = 'Event' THEN 'Event' 
		ELSE 'Task' 
	    END AS Appointment_type,
	    todo.reference_type AS Reference_Type,
	    todo.Yesterday,
	    todo.Today,
	    todo.Tomorrow,
	    todo.Week,
	    COALESCE(co.Count_Status_Open, 0) AS Count_Status_Open
	FROM ranked_data st
	LEFT JOIN `tabBusiness Deverloper` bd ON bd.parent = st.name
	LEFT JOIN filtered_sales_person sp ON sp.custom_user = bd.bd
	LEFT JOIN final_todo_data todo ON LOWER(todo.owner) = LOWER(bd.bd)
	LEFT JOIN todo_status_count co ON bd.bd= co.owner
	WHERE st.rank = 1
      AND ((bd.zonal_head = %s) OR (bd.bd = %s) OR (st.business_head = %s))
      ORDER BY Team, BD
    """
    
    # Execute the query
    data = frappe.db.sql(query,(frappe.session.user, frappe.session.user, frappe.session.user), as_dict=True)

    # Define the columns
    columns = [
        {"fieldname": "Team", "label": "Team Name", "fieldtype": "Data", "width": 150},
        #{"fieldname": "Team_Username", "label": "Team Username", "fieldtype": "Data", "width": 150},
        {"fieldname": "BD", "label": "BD Name", "fieldtype": "Data", "width": 200},
        {"fieldname": "Zonal_head", "label": "Zonal Head", "fieldtype": "Data", "width": 150},
        {"fieldname": "Todo_ID", "label": "Todo ID", "fieldtype": "Data", "width": 150},
        {"fieldname": "Appointment_type", "label": "Appointment Type", "fieldtype": "Data", "width": 200},
        {"fieldname": "Reference_Type", "label": "Reference Type", "fieldtype": "Data", "width": 150},
        {"fieldname": "Yesterday", "label": "Yesterday", "fieldtype": "Data", "width": 150},
        {"fieldname": "Today", "label": "Today", "fieldtype": "Data", "width": 200},
        {"fieldname": "Week", "label": "Week", "fieldtype": "Data", "width": 150},
        {"fieldname": "Count_Status_Open", "label": "Count Status Open ", "fieldtype": "Data", "width": 200}
        
    ]
    
    # Return columns and data
    return columns, data


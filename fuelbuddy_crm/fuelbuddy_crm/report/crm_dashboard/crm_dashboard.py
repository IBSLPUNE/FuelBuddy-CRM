import frappe
from frappe.utils import today

def execute(filters=None):
    query = """
    SELECT
	    custom_team,
	    zonal_head_sales_person,
	    bd_sales_person, 
	    Category,
	    FORMAT(`LWD Target`, 2) AS `LWD_Target`,
	    FORMAT(`LWD Actual`, 2) AS `LWD_Actual`,
	    FORMAT(`WTD Target`, 2) AS `WTD_Target`, 
	    FORMAT(`WTD Actual`, 2) AS `WTD_Actual`,
	    FORMAT(`MTD Target`, 2) AS `MTD_Target`,
	    FORMAT(`MTD Actual`, 2) AS `MTD_Actual`,
	    FORMAT(`QTD Target`, 2) AS `QTD_Target`,
	    FORMAT(`QTD Actual`, 2) AS `QTD_Actual`
	    
	FROM (
	    SELECT
		sp.custom_team,
		bd.zonal_head_sales_person,
		bd.bd_sales_person,
		bd.bd,
		'Orders' AS `Category`,
		MAX(CASE 
		    WHEN DATE(da.date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) 
		    THEN da.daily_target
		    ELSE 0 
		END) AS `LWD Target`,
	    MAX(CASE 
		WHEN DATE(da.date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) 
		THEN da.daily_achievement_sales_order 
		ELSE 0 
	    END) AS `LWD Actual`,
		MAX(CASE 
		    WHEN WEEK(wa.date) = WEEK(CURDATE()) AND YEAR(wa.date) = YEAR(CURDATE()) 
		    THEN wa.weekly_target 
		    ELSE 0 
		END) AS `WTD Target`,
		MAX(CASE 
		    WHEN WEEK(wa.date) = WEEK(CURDATE()) AND YEAR(wa.date) = YEAR(CURDATE()) 
		    THEN wa.daily_achievement_sales_order 
		    ELSE 0 
		END) AS `WTD Actual`,
		MAX(CASE 
		WHEN MONTH(ma.date) = MONTH(CURDATE()) AND YEAR(ma.date) = YEAR(CURDATE()) 
		THEN ma.monthly_target 
		ELSE 0 
	    END) AS `MTD Target`,
	    MAX(CASE 
		WHEN MONTH(ma.date) = MONTH(CURDATE()) AND YEAR(ma.date) = YEAR(CURDATE()) 
		THEN ma.daily_achievement_sales_order 
		ELSE 0 
	    END) AS `MTD Actual`,
	    MAX(CASE 
		WHEN QUARTER(qa.date) = QUARTER(CURDATE()) AND YEAR(qa.date) = YEAR(CURDATE()) 
		THEN qa.quaterly_target
		ELSE 0 
	    END) AS `QTD Target`,
	    MAX(CASE 
		WHEN QUARTER(qa.date) = QUARTER(CURDATE()) AND YEAR(qa.date) = YEAR(CURDATE()) 
		THEN qa.quaterly_achievement_sales_order
		ELSE 0 
	    END) AS `QTD Actual`
	    FROM
		`tabSales Target` st
	    LEFT JOIN
		`tabBusiness Deverloper` bd ON bd.parent = st.name
	    LEFT JOIN
		`tabSales Person` sp ON sp.name= bd.bd_sales_person
	    LEFT JOIN
		`tabWeekly Achievement` wa ON wa.bd = bd.bd
	    LEFT JOIN 
		`tabDaily Achievement` da ON da.bd = bd.bd
	    LEFT JOIN 
	    `tabMonthly Achievement` ma ON ma.bd = bd.bd
	LEFT JOIN 
	    `tabQuaterly Achievement` qa ON qa.bd = bd.bd
	    WHERE
		((wa.date BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE())
		OR DATE(da.date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
		OR (MONTH(ma.date) = MONTH(CURDATE()) AND YEAR(ma.date) = YEAR(CURDATE()))
	    OR (QUARTER(qa.date) = QUARTER(CURDATE()) AND YEAR(qa.date) = YEAR(CURDATE())))
	    AND (bd.bd = %s OR bd.zonal_head = %s OR st.business_head = %s)
	    GROUP BY 
		bd.bd

	    UNION ALL

	    SELECT
		sp.custom_team,
		bd.zonal_head_sales_person,
		bd.bd_sales_person,
		bd.bd,
		'Deliveries' AS `Category`,
		MAX(CASE 
		    WHEN DATE(da.date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) 
		    THEN da.daily_target
		    ELSE 0 
		END) AS `LWD Target`,
	    MAX(CASE 
		WHEN DATE(da.date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) 
		THEN da.daily_achievement 
		ELSE 0 
	    END) AS `LWD Actual`,
		MAX(CASE 
		    WHEN WEEK(wa.date) = WEEK(CURDATE()) AND YEAR(wa.date) = YEAR(CURDATE()) 
		    THEN wa.weekly_target 
		    ELSE 0 
		END) AS `WTD Target`,
		MAX(CASE 
		    WHEN WEEK(wa.date) = WEEK(CURDATE()) AND YEAR(wa.date) = YEAR(CURDATE()) 
		    THEN wa.weekly_achievement
		    ELSE 0 
		END) AS `WTD Actual`,
		MAX(CASE 
		WHEN MONTH(ma.date) = MONTH(CURDATE()) AND YEAR(ma.date) = YEAR(CURDATE()) 
		THEN ma.monthly_target 
		ELSE 0 
	    END) AS `MTD Target`,
	    MAX(CASE 
		WHEN MONTH(ma.date) = MONTH(CURDATE()) AND YEAR(ma.date) = YEAR(CURDATE()) 
		THEN ma.monthly_achievement 
		ELSE 0 
	    END) AS `MTD Actual`,
	    MAX(CASE 
		WHEN QUARTER(qa.date) = QUARTER(CURDATE()) AND YEAR(qa.date) = YEAR(CURDATE()) 
		THEN qa.quaterly_target
		ELSE 0 
	    END) AS `QTD Target`,
	    MAX(CASE 
		WHEN QUARTER(qa.date) = QUARTER(CURDATE()) AND YEAR(qa.date) = YEAR(CURDATE()) 
		THEN qa.quaterly_achievement 
		ELSE 0 
	    END) AS `QTD Actual`
	    FROM
		`tabSales Target` st
	    LEFT JOIN
		`tabBusiness Deverloper` bd ON bd.parent = st.name
	    LEFT JOIN
		`tabSales Person` sp ON sp.name= bd.bd_sales_person
	    LEFT JOIN
		`tabWeekly Achievement` wa ON wa.bd = bd.bd
	    LEFT JOIN 
	    `tabDaily Achievement` da ON da.bd = bd.bd
	    LEFT JOIN 
	    `tabMonthly Achievement` ma ON ma.bd = bd.bd
	LEFT JOIN 
	    `tabQuaterly Achievement` qa ON qa.bd = bd.bd
	    WHERE
		((wa.date BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE())
		OR DATE(da.date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
		OR (MONTH(ma.date) = MONTH(CURDATE()) AND YEAR(ma.date) = YEAR(CURDATE()))
	    OR (QUARTER(qa.date) = QUARTER(CURDATE()) AND YEAR(qa.date) = YEAR(CURDATE())))
	    AND (bd.bd = %s OR bd.zonal_head = %s OR st.business_head = %s)
	    GROUP BY 
		bd.bd
	 UNION ALL
	 SELECT
		sp.custom_team,
		bd.zonal_head_sales_person,
		bd.bd_sales_person,
		bd.bd,
		'Collection' AS `Category`,
		MAX(CASE 
		    WHEN DATE(da.date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) 
		    THEN da.daily_target*90
		    ELSE 0 
		END) AS `LWD Target`,
	    MAX(CASE 
		WHEN DATE(da.date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) 
		THEN da.collection 
		ELSE 0 
	    END) AS `LWD Actual`,
		MAX(CASE 
		    WHEN WEEK(wa.date) = WEEK(CURDATE()) AND YEAR(wa.date) = YEAR(CURDATE()) 
		    THEN wa.weekly_target*90 
		    ELSE 0 
		END) AS `WTD Target`,
		MAX(CASE 
		    WHEN WEEK(wa.date) = WEEK(CURDATE()) AND YEAR(wa.date) = YEAR(CURDATE()) 
		    THEN wa.collection 
		    ELSE 0 
		END) AS `WTD Actual`,
		MAX(CASE 
		WHEN MONTH(ma.date) = MONTH(CURDATE()) AND YEAR(ma.date) = YEAR(CURDATE()) 
		THEN ma.monthly_target*90 
		ELSE 0 
	    END) AS `MTD Target`,
	    MAX(CASE 
		WHEN MONTH(ma.date) = MONTH(CURDATE()) AND YEAR(ma.date) = YEAR(CURDATE()) 
		THEN ma.collection
		ELSE 0 
	    END) AS `MTD Actual`,
	    MAX(CASE 
		WHEN QUARTER(qa.date) = QUARTER(CURDATE()) AND YEAR(qa.date) = YEAR(CURDATE()) 
		THEN qa.quaterly_target*90
		ELSE 0 
	    END) AS `QTD Target`,
	    MAX(CASE 
		WHEN QUARTER(qa.date) = QUARTER(CURDATE()) AND YEAR(qa.date) = YEAR(CURDATE()) 
		THEN qa.collection
		ELSE 0 
	    END) AS `QTD Actual`
	    FROM
		`tabSales Target` st
	    LEFT JOIN
		`tabBusiness Deverloper` bd ON bd.parent = st.name
	    LEFT JOIN
		`tabSales Person` sp ON sp.name= bd.bd_sales_person
	    LEFT JOIN
		`tabWeekly Achievement` wa ON wa.bd = bd.bd
	    LEFT JOIN 
		`tabDaily Achievement` da ON da.bd = bd.bd
	    LEFT JOIN 
	    `tabMonthly Achievement` ma ON ma.bd = bd.bd
	LEFT JOIN 
	    `tabQuaterly Achievement` qa ON qa.bd = bd.bd
	    WHERE
		((wa.date BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE())
		OR DATE(da.date) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
		OR (MONTH(ma.date) = MONTH(CURDATE()) AND YEAR(ma.date) = YEAR(CURDATE()))
	    OR (QUARTER(qa.date) = QUARTER(CURDATE()) AND YEAR(qa.date) = YEAR(CURDATE())))
	    AND (bd.bd = %s OR bd.zonal_head = %s OR st.business_head = %s)
	    GROUP BY 
		bd.bd
	) AS combined_data
	ORDER BY 
	    custom_team,zonal_head_sales_person,bd_sales_person,
	    FIELD(Category, 'Orders', 'Deliveries', 'Collection');
    """
    data = frappe.db.sql(query, (frappe.session.user, frappe.session.user, frappe.session.user,frappe.session.user, frappe.session.user, frappe.session.user,frappe.session.user, frappe.session.user, frappe.session.user), as_dict=True)
    columns = [
        {"fieldname": "custom_team", "label": "Team", "fieldtype": "Data", "width":150},
        #{"fieldname": "slp", "label": "Sales Person", "fieldtype": "Data"},
        #{"fieldname": "bushead", "label": "Bussiness Head", "fieldtype": "Data"},
        {"fieldname": "zonal_head_sales_person", "label": "Zonal Head", "fieldtype": "Data", "width":150},
        #{"fieldname": "Head_Name", "label": "Zonal Head Name", "fieldtype": "Data"},
        #{"fieldname": "Team_Username", "label": "Team Username", "fieldtype": "Data"},
        {"fieldname": "bd_sales_person", "label": "BD", "fieldtype": "Data", "width":150},
        #{"fieldname": "bd", "label": "BD", "fieldtype": "Data"},
        {"fieldname": "Category", "label": "Category", "fieldtype": "Data", "width":100},
        {"fieldname": "LWD_Target", "label": "LWD Target", "fieldtype": "Float", "width":150, "precision": 2},
        {"fieldname": "LWD_Actual", "label": "LWD Achv.", "fieldtype": "Float", "width":150, "precision": 2},
        #{"fieldname": "LWD_ATT", "label": "LWD ATT %", "fieldtype": "Percent"},
        {"fieldname": "WTD_Target", "label": "WTD Target", "fieldtype": "Float", "width":150, "precision": 2},
        {"fieldname": "WTD_Actual", "label": "WTD Achv.", "fieldtype": "Float", "width":150, "precision": 2},
        #{"fieldname": "WEEK_Actual", "label": "Actual Lacs", "fieldtype": "Float"},
        #{"fieldname": "WEEK_ATT", "label": "Weekly ATT %", "fieldtype": "Float"},
        {"fieldname": "MTD_Target", "label": "MTD Target", "fieldtype": "Float", "width":150, "precision": 2},
        {"fieldname": "MTD_Actual", "label": "MTD Achv.", "fieldtype": "Float", "width":150, "precision": 2},
        #{"fieldname": "MTD_Actual", "label": "Actual Lacs", "fieldtype": "Float"},
        {"fieldname": "QTD_Target", "label": "QTD Target", "fieldtype": "Float", "width":150, "precision": 2},
        {"fieldname": "QTD_Actual", "label": "QTD Achv.", "fieldtype": "Float", "width":150, "precision": 2}
    ]
    return columns, data


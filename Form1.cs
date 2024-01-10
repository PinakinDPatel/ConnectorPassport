using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using System.Data.SqlClient;
using System.Configuration;
using System.Xml;
using System.Threading;
using System.Net;
using Newtonsoft.Json;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Xml.Linq;
using System.Xml.Serialization;
using Renci.SshNet;
using Renci.SshNet.Sftp;

namespace Connecter
{
    public partial class Form1 : Form
    {
        string conString = "Server=" + ConfigurationManager.AppSettings["ServerName"].ToString() + "; Database=" + ConfigurationManager.AppSettings["DBName"].ToString() + "; User Id=pspcstore; Password=Prem#12681#; Trusted_Connection=False; MultipleActiveResultSets=true";
        string storeId = AppCommon.StoreId(ConfigurationManager.AppSettings.Get("Key"));
        string posId = AppCommon.POSId(ConfigurationManager.AppSettings.Get("Key"));
        string sourcePath = ConfigurationManager.AppSettings.Get("SourcePath");
        string outPath = ConfigurationManager.AppSettings.Get("OutPath");
        bool isSend = false;
        public Form1()
        {
            Task task = new Task(() =>
            {
                while (true)
                {
                    var dayName = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("dddd");
                    var timeHour = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("HH");
                    var timeMin = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("mm");
                    if (dayName == "Tuesday" && isSend == false)
                    {
                        // For ScanData.
                        ScanDataRJR();
                        ScanDataAltria();
                        isSend = true;
                    }
                    else
                    {
                        if (sourcePath != null && storeId != null && posId != null && outPath != null)
                        {
                            PassportWrite();
                            PassportRead();
                        }
                    }
                    if (dayName != "TuesDay")
                    {
                        isSend = false;
                    }
                    //Progress();
                    Thread.Sleep(10000);
                }
            });
            task.Start();
            InitializeComponent();
        }

        private void PassportWrite()
        {
            SqlConnection conn = new SqlConnection(conString);
            string errorFileName = "";
            try
            {
                if (posId != null && storeId != null && outPath != null)
                {
                    string query = "select id,xmlname,xmlcontent,Storeid from  xmlout where Storeid='" + storeId + "'";
                    conn.Open();
                    DataTable dtxml = new DataTable();
                    SqlDataAdapter adp = new SqlDataAdapter(query, conn);
                    adp.SelectCommand.CommandTimeout = 100000;
                    adp.Fill(dtxml);
                    conn.Close();

                    if (dtxml.Rows.Count > 0)
                    {
                        for (int i = 0; dtxml.Rows.Count >= i + 1; i++)
                        {
                            var outxmlpath = outPath + dtxml.Rows[i]["xmlname"].ToString();
                            XmlTextWriter writer = new XmlTextWriter(outxmlpath, System.Text.Encoding.UTF8);
                            DataSet dsxmlcontet = new DataSet();
                            string encodedXml = WebUtility.HtmlDecode(dtxml.Rows[i]["xmlcontent"].ToString()
                                .Replace("&", "&amp;").Replace("'", "&apos;").Replace("\"", "&quot;").Replace(">", "&gt;").Replace("<", "&lt;"));
                            writer.Close();
                            XmlDocument xdoc = new XmlDocument();
                            xdoc.LoadXml(encodedXml);
                            xdoc.Save(outxmlpath);
                            int id = Convert.ToInt32(dtxml.Rows[i]["id"]);
                            var cmd = conn.CreateCommand();
                            conn.Open();
                            cmd.CommandText = "DELETE FROM XmlOut WHERE storeid = @storeId and xmlname = @xmlname and id= @id";
                            cmd.Parameters.AddWithValue("@storeId", storeId);
                            cmd.Parameters.AddWithValue("@id", id);
                            cmd.Parameters.AddWithValue("@xmlname", dtxml.Rows[i]["xmlname"].ToString());
                            cmd.ExecuteNonQuery();
                            conn.Close();
                        }
                        //var xmlListFile = CreateXmlListFile(dtxml);

                    }
                }
            }
            catch (Exception ex)
            {
                SendErrorToText(ex, errorFileName);
            }
        }

        public string CreateXmlListFile(DataTable result)
        {
            var xmlSerializer = new XmlSerializer(result.GetType());
            var xdoc = new XDocument(
                       new XElement("FileList",
                       result.AsEnumerable().Select(w =>
                       new XElement("File", w["xmlname"]))));

            try
            {
                var outxmlpath = outPath + "FILEMANIFEST_" + DateTime.Now.ToString("yyMMddHHmmss").ToString();
                XmlTextWriter writer = new XmlTextWriter(outxmlpath, System.Text.Encoding.UTF8);
                DataSet dsxmlcontet = new DataSet();
                string encodedXml = WebUtility.HtmlDecode(xdoc.ToString());
                writer.Close();
                XmlDocument xdoc1 = new XmlDocument();
                xdoc1.LoadXml(encodedXml);
                xdoc1.Save(outxmlpath);
            }
            catch (Exception e)
            {
                throw e;
            }

            return xdoc.ToString();
        }

        private void PassportRead()
        {
            SqlConnection conn = new SqlConnection(conString);
            string errorFileName = "";
            try
            {
                if (sourcePath != null && storeId != null && posId != null)
                {
                    foreach (var XMlFile in System.IO.Directory.GetFiles(sourcePath))
                    {
                        if (File.Exists(XMlFile))
                        {
                            var folderName = "";
                            errorFileName = XMlFile;
                            DataSet ds = new DataSet();
                            ds.ReadXml(XMlFile);
                            FileInfo fileinfo = new FileInfo(XMlFile);
                            string ss = fileinfo.Name.Replace(fileinfo.Extension, "");
                            string str = ss.Substring(0, 3);
                            int icount = ds.Tables.Count;
                            //string xmlString = System.IO.File.ReadAllText(XMlFile);
                            try
                            {
                                if (str == "MCM")
                                {
                                    String Date = ds.Tables["MovementHeader"].Rows[0]["EndDate"].ToString();
                                    folderName = Date;
                                    string queryMCM = "select distinct EndDate,StoreId,POSId,MCMDetail_Id from MCM where Storeid='" + storeId + "' and POSId='" + posId + "' and EndDate='" + Date + "'";
                                    conn.Open();
                                    DataTable dtmcm = new DataTable();
                                    SqlDataAdapter adtmcm = new SqlDataAdapter(queryMCM, conn);
                                    adtmcm.SelectCommand.CommandTimeout = 100000;
                                    adtmcm.Fill(dtmcm);
                                    conn.Close();
                                    if (ds.Tables.Contains("MCMSalesTotals"))
                                    {
                                        for (int i = 0; i < ds.Tables["MCMSalesTotals"].Rows.Count; i++)
                                        {
                                            string BeginDate = ds.Tables["MovementHeader"].Rows[0]["BeginDate"].ToString();
                                            string BeginTime = ds.Tables["MovementHeader"].Rows[0]["BeginTime"].ToString();
                                            string EndDate = ds.Tables["MovementHeader"].Rows[0]["EndDate"].ToString();
                                            string EndTime = ds.Tables["MovementHeader"].Rows[0]["EndTime"].ToString();
                                            string MerchandiseCode = ds.Tables["MCMDetail"].Rows[i]["MerchandiseCode"].ToString();
                                            string MerchandiseCodeDescription = ds.Tables["MCMDetail"].Rows[i]["MerchandiseCodeDescription"].ToString();
                                            string MCMDetail_Id = ds.Tables["MCMDetail"].Rows[i]["MCMDetail_Id"].ToString();
                                            string DiscountAmount = ds.Tables["MCMSalesTotals"].Rows[i]["DiscountAmount"].ToString();
                                            string DiscountCount = ds.Tables["MCMSalesTotals"].Rows[i]["DiscountCount"].ToString();
                                            string PromotionAmount = ds.Tables["MCMSalesTotals"].Rows[i]["PromotionAmount"].ToString();
                                            string PromotionCount = ds.Tables["MCMSalesTotals"].Rows[i]["PromotionCount"].ToString();
                                            string RefundAmount = ds.Tables["MCMSalesTotals"].Rows[i]["RefundAmount"].ToString();
                                            string RefundCount = ds.Tables["MCMSalesTotals"].Rows[i]["RefundCount"].ToString();
                                            string SalesQuantity = ds.Tables["MCMSalesTotals"].Rows[i]["SalesQuantity"].ToString();
                                            string SalesAmount = ds.Tables["MCMSalesTotals"].Rows[i]["SalesAmount"].ToString();
                                            string TransactionCount = ds.Tables["MCMSalesTotals"].Rows[i]["TransactionCount"].ToString();
                                            string OpenDepartmentSalesAmount = ds.Tables["MCMSalesTotals"].Rows[i]["OpenDepartmentSalesAmount"].ToString();
                                            string OpenDepartmentTransactionCount = ds.Tables["MCMSalesTotals"].Rows[i]["OpenDepartmentTransactionCount"].ToString();
                                            int pjrflg = 1;
                                            if (dtmcm.Rows.Count == 0)
                                            {
                                                pjrflg = 0;
                                            }
                                            else
                                            {
                                                for (int k = 0; k < dtmcm.Rows.Count; k++)
                                                {
                                                    if (dtmcm.Rows[k]["EndDate"].ToString() == EndDate && dtmcm.Rows[k]["MCMDetail_Id"].ToString() == MCMDetail_Id && dtmcm.Rows[k]["StoreId"].ToString() == storeId)
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        pjrflg = 0;
                                                    }
                                                }
                                            }
                                            if (pjrflg != 1)
                                            {
                                                string Query = "insert into MCM VALUES('" + BeginDate + "','" + BeginTime + "','" + EndDate + "','" + EndTime + "','" + MerchandiseCode + "','" + MerchandiseCodeDescription + "','" + MCMDetail_Id + "','" + DiscountAmount + "','" + DiscountCount + "','" + PromotionAmount + "','" + PromotionCount + "','" + RefundAmount + "','" + RefundCount + "','" + SalesQuantity + "','" + SalesAmount + "','" + TransactionCount + "','" + OpenDepartmentSalesAmount + "','" + OpenDepartmentTransactionCount + "','" + storeId + "','" + posId + "')";
                                                conn.Open();
                                                SqlCommand cmd = new SqlCommand(Query, conn);
                                                cmd.ExecuteNonQuery();
                                                conn.Close();
                                            }
                                        }
                                    }
                                }
                                if (str == "MSM")
                                {
                                    string Date = ds.Tables["MovementHeader"].Rows[0]["EndDate"].ToString();
                                    folderName = Date;
                                    string queryMSM = "select EndDate,StoreId,POSId,MSMDetail_Id from MSM where Storeid='" + storeId + "' and POSId='" + posId + "' and EndDate='" + Date + "'";
                                    conn.Open();
                                    DataTable dtmsm = new DataTable();
                                    SqlDataAdapter adtmsm = new SqlDataAdapter(queryMSM, conn);
                                    adtmsm.SelectCommand.CommandTimeout = 100000;
                                    adtmsm.Fill(dtmsm);
                                    conn.Close();
                                    if (ds.Tables.Contains("MSMDetail"))
                                    {
                                        for (int i = 0; i < ds.Tables["MSMDetail"].Rows.Count; i++)
                                        {
                                            string BeginDate = ds.Tables["MovementHeader"].Rows[0]["BeginDate"].ToString();
                                            string BeginTime = ds.Tables["MovementHeader"].Rows[0]["BeginTime"].ToString();
                                            string EndDate = ds.Tables["MovementHeader"].Rows[0]["EndDate"].ToString();
                                            string EndTime = ds.Tables["MovementHeader"].Rows[0]["EndTime"].ToString();
                                            string MSMDetail_Id = ds.Tables["MSMDetail"].Rows[i]["MSMDetail_Id"].ToString();
                                            string MiscellaneousSummaryCode = ds.Tables["MiscellaneousSummaryCodes"].Rows[i]["MiscellaneousSummaryCode"].ToString();
                                            string MiscellaneousSummarySubCode = ds.Tables["MiscellaneousSummaryCodes"].Rows[i]["MiscellaneousSummarySubCode"].ToString();
                                            string MiscellaneousSummarySubCodeModifier = ds.Tables["MiscellaneousSummaryCodes"].Rows[i]["MiscellaneousSummarySubCodeModifier"].ToString();
                                            string MSMSalesTotals_Id = ds.Tables["MSMSalesTotals"].Rows[i]["MSMSalesTotals_Id"].ToString();
                                            string MiscellaneousSummaryAmount = ds.Tables["MSMSalesTotals"].Rows[i]["MiscellaneousSummaryAmount"].ToString();
                                            string MiscellaneousSummaryCount = ds.Tables["MSMSalesTotals"].Rows[i]["MiscellaneousSummaryCount"].ToString();
                                            string TenderCode = ds.Tables["Tender"].Rows[i]["TenderCode"].ToString();
                                            string TenderSubCode = ds.Tables["Tender"].Rows[i]["TenderSubCode"].ToString();
                                            int pjrflg = 1;
                                            if (dtmsm.Rows.Count == 0)
                                            {
                                                pjrflg = 0;
                                            }
                                            else
                                            {
                                                for (int k = 0; k < dtmsm.Rows.Count; k++)
                                                {
                                                    if (dtmsm.Rows[k]["EndDate"].ToString() == EndDate && dtmsm.Rows[k]["MSMDetail_Id"].ToString() == MSMDetail_Id && dtmsm.Rows[k]["StoreId"].ToString() == storeId)
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        pjrflg = 0;
                                                    }
                                                }
                                            }
                                            if (pjrflg != 1)
                                            {
                                                string Query = "insert into MSM VALUES('" + BeginDate + "','" + BeginTime + "','" + EndDate + "','" + EndTime + "','" + MSMDetail_Id + "','" + MiscellaneousSummaryCode + "','" + MiscellaneousSummarySubCode + "','" + MiscellaneousSummarySubCodeModifier + "','" + MSMSalesTotals_Id + "','" + MiscellaneousSummaryAmount + "','" + MiscellaneousSummaryCount + "','" + TenderCode + "','" + TenderSubCode + "','" + storeId + "','" + posId + "')";
                                                conn.Open();
                                                SqlCommand cmd = new SqlCommand();
                                                cmd = new SqlCommand(Query, conn);
                                                cmd.ExecuteNonQuery();
                                                conn.Close();
                                            }
                                        }
                                    }
                                }
                                if (str == "PJR")
                                {
                                    string EventSequenceID = null;
                                    string SaleEvent_Id = null;
                                    string CashierID = null;
                                    string RegisterID = null;
                                    string TillID = null;
                                    string TransactionID = null;
                                    string EventStartDate = null;
                                    string EventStartTime = null;
                                    string EventEndDate = null;
                                    string EventEndTime = null;
                                    string BusinessDate = null;
                                    string ReceiptDate = null;
                                    string ReceiptTime = null;
                                    string value = null;
                                    string status = null;
                                    string TenderInfo_Id = null;
                                    string TenderAmount = null;
                                    string TenderCode = null;
                                    string TenderSubCode = null;
                                    string AccountName = null;
                                    string SalesAmount = null;
                                    string TaxLevelID = null;
                                    string ItemLine_Id = null;
                                    string Description = null;
                                    string ActualSalesPrice = null;
                                    string MerchandiseCode = null;
                                    string SellingUnits = null;
                                    string RegularSellPrice = null;
                                    string SalesQuantity = null;
                                    string ItemCode_Id = null;
                                    string POSCode = null;
                                    string format = null;
                                    string method = null;
                                    string TransactionTotalGrossAmount = null;
                                    string TransactionTotalNetAmount = null;
                                    string TransactionTotalTaxSalesAmount = null;
                                    string TransactionTotalTaxExemptAmount = null;
                                    string TransactionTotalTaxNetAmount = null;
                                    string TransactionSummary_Id = null;
                                    string TransactionTotalGrandAmount_Text = null;
                                    string FuelLine_id = null;
                                    string MerchandiseCode_id = null;
                                    string M_Description = null;
                                    string M_ActualSalesPrice = null;
                                    string M_MerchandiseCode = null;
                                    string M_RegularSellPrice = null;
                                    string M_SalesQuantity = null;
                                    string M_SalesAmount = null;
                                    string F_FuelGradeID = null;
                                    string F_FuelpostionID = null;
                                    string F_PriceTierCode = null;
                                    string F_TimeTierCode = null;
                                    string F_ServiceLevelCode = null;
                                    string F_Description = null;
                                    string F_ActualSalesPrice = null;
                                    string F_MerchandiseCode = null;
                                    string F_RegularSellPrice = null;
                                    string F_SalesQuantity = null;
                                    string F_SalesAmount = null;
                                    string SuspendFlag_value = null;
                                    string date = null;
                                    string traId = null;
                                    bool voidTrans = false;
                                    string PromotionID = null;
                                    string PromotionAmount = null;
                                    string DiscountAmount = null;

                                    if (ds.Tables.Contains("SaleEvent")) { date = ds.Tables["SaleEvent"].Rows[0]["EventEndDate"].ToString(); traId = ds.Tables["SaleEvent"].Rows[0]["TransactionID"].ToString(); }
                                    else if (ds.Tables.Contains("OtherEvent")) { date = ds.Tables["OtherEvent"].Rows[0]["EventEndDate"].ToString(); traId = ds.Tables["OtherEvent"].Rows[0]["TransactionID"].ToString(); }
                                    else if (ds.Tables.Contains("VoidEvent")) { date = ds.Tables["VoidEvent"].Rows[0]["EventEndDate"].ToString(); traId = ds.Tables["VoidEvent"].Rows[0]["TransactionID"].ToString(); }
                                    else if (ds.Tables.Contains("FinancialEvent")) { date = ds.Tables["FinancialEvent"].Rows[0]["EventEndDate"].ToString(); traId = ds.Tables["FinancialEvent"].Rows[0]["TransactionID"].ToString(); }
                                    else if (ds.Tables.Contains("RefundEvent")) { date = ds.Tables["RefundEvent"].Rows[0]["EventEndDate"].ToString(); traId = ds.Tables["RefundEvent"].Rows[0]["TransactionID"].ToString(); }
                                    folderName = date;
                                    string queryTrans = "select TransactionID,EventEndTime,Storeid,POSId from TransactionPJR where Storeid='" + storeId + "' and POSId='" + posId + "' and EventEndDate='" + date + "' and TransactionId='" + traId + "'";
                                    conn.Open();
                                    DataTable dtTrans = new DataTable();
                                    SqlDataAdapter adp = new SqlDataAdapter(queryTrans, conn);
                                    adp.SelectCommand.CommandTimeout = 100000;
                                    adp.Fill(dtTrans);
                                    conn.Close();

                                    string queryFuel = "select FuelLine_id,Storeid,TransactionID,EventEndDate,POSId from FuelPJR where Storeid='" + storeId + "' and POSId='" + posId + "'and EventEndDate='" + date + "' and TransactionId='" + traId + "'";
                                    conn.Open();
                                    DataTable dtFual = new DataTable();
                                    SqlDataAdapter adpfual = new SqlDataAdapter(queryFuel, conn);
                                    adpfual.SelectCommand.CommandTimeout = 100000;
                                    adpfual.Fill(dtFual);
                                    conn.Close();

                                    string queryitem = "select ItemLine_Id,Storeid,TransactionID,EventEndDate,POSId from itemPJR where Storeid='" + storeId + "' and POSId='" + posId + "'and EventEndDate='" + date + "' and TransactionId='" + traId + "'";
                                    conn.Open();
                                    DataTable dtitem = new DataTable();
                                    SqlDataAdapter adpitem = new SqlDataAdapter(queryitem, conn);
                                    adpitem.SelectCommand.CommandTimeout = 100000;
                                    adpitem.Fill(dtitem);
                                    conn.Close();

                                    string queryMer = "select MerchandiseCode,Storeid,TransactionID,EventEndDate,POSId from MerchandisePJR where Storeid='" + storeId + "' and POSId='" + posId + "'and EventEndDate='" + date + "' and TransactionId='" + traId + "'";
                                    conn.Open();
                                    DataTable dtmer = new DataTable();
                                    SqlDataAdapter admer = new SqlDataAdapter(queryMer, conn);
                                    admer.SelectCommand.CommandTimeout = 100000;
                                    admer.Fill(dtmer);
                                    conn.Close();

                                    string queryTend = "select TenderInfo_Id,Storeid,TransactionID,EventEndDate,POSId from TenderPJR where Storeid='" + storeId + "' and POSId='" + posId + "'and EventEndDate='" + date + "' and TransactionId='" + traId + "'";
                                    conn.Open();
                                    DataTable dttend = new DataTable();
                                    SqlDataAdapter adtend = new SqlDataAdapter(queryTend, conn);
                                    adtend.SelectCommand.CommandTimeout = 100000;
                                    adtend.Fill(dttend);
                                    conn.Close();

                                    int pjrflg = 1;
                                    for (int i = 0; i < 1; i++)
                                    {
                                        if (ds.Tables.Contains("SaleEvent"))
                                        {
                                            if (ds.Tables["SaleEvent"].Columns.Contains("EventSequenceID"))
                                                EventSequenceID = ds.Tables["SaleEvent"].Rows[i]["EventSequenceID"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("SaleEvent_Id"))
                                                SaleEvent_Id = ds.Tables["SaleEvent"].Rows[i]["SaleEvent_Id"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("CashierID"))
                                                CashierID = ds.Tables["SaleEvent"].Rows[i]["CashierID"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("RegisterID"))
                                                RegisterID = ds.Tables["SaleEvent"].Rows[i]["RegisterID"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("TillID"))
                                                TillID = ds.Tables["SaleEvent"].Rows[i]["TillID"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("TransactionID"))
                                                TransactionID = ds.Tables["SaleEvent"].Rows[i]["TransactionID"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("EventStartDate"))
                                                EventStartDate = ds.Tables["SaleEvent"].Rows[i]["EventStartDate"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("EventStartTime"))
                                                EventStartTime = ds.Tables["SaleEvent"].Rows[i]["EventStartTime"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("EventEndDate"))
                                                EventEndDate = ds.Tables["SaleEvent"].Rows[i]["EventEndDate"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("EventEndTime"))
                                                EventEndTime = ds.Tables["SaleEvent"].Rows[0]["EventEndTime"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("BusinessDate"))
                                                BusinessDate = ds.Tables["SaleEvent"].Rows[0]["BusinessDate"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("ReceiptDate"))
                                                ReceiptDate = ds.Tables["SaleEvent"].Rows[0]["ReceiptDate"].ToString();
                                            if (ds.Tables["SaleEvent"].Columns.Contains("ReceiptTime"))
                                                ReceiptTime = ds.Tables["SaleEvent"].Rows[0]["ReceiptTime"].ToString();
                                            if (ds.Tables["SuspendFlag"].Columns.Contains("value"))
                                                SuspendFlag_value = ds.Tables["SuspendFlag"].Rows[0]["value"].ToString();
                                        }
                                        else if (ds.Tables.Contains("OtherEvent"))
                                        {
                                            if (ds.Tables["OtherEvent"].Columns.Contains("EventSequenceID"))
                                                EventSequenceID = ds.Tables["OtherEvent"].Rows[0]["EventSequenceID"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("SaleEvent_Id"))
                                                SaleEvent_Id = ds.Tables["OtherEvent"].Rows[0]["SaleEvent_Id"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("CashierID"))
                                                CashierID = ds.Tables["OtherEvent"].Rows[0]["CashierID"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("RegisterID"))
                                                RegisterID = ds.Tables["OtherEvent"].Rows[0]["RegisterID"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("TillID"))
                                                TillID = ds.Tables["OtherEvent"].Rows[0]["TillID"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("TransactionID"))
                                                TransactionID = ds.Tables["OtherEvent"].Rows[0]["TransactionID"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("EventStartDate"))
                                                EventStartDate = ds.Tables["OtherEvent"].Rows[0]["EventStartDate"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("EventStartTime"))
                                                EventStartTime = ds.Tables["OtherEvent"].Rows[0]["EventStartTime"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("EventEndDate"))
                                                EventEndDate = ds.Tables["OtherEvent"].Rows[0]["EventEndDate"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("EventEndTime"))
                                                EventEndTime = ds.Tables["OtherEvent"].Rows[0]["EventEndTime"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("BusinessDate"))
                                                BusinessDate = ds.Tables["OtherEvent"].Rows[0]["BusinessDate"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("ReceiptDate"))
                                                ReceiptDate = ds.Tables["SaleEvent"].Rows[0]["ReceiptDate"].ToString();
                                            if (ds.Tables["OtherEvent"].Columns.Contains("ReceiptTime"))
                                                ReceiptTime = ds.Tables["OtherEvent"].Rows[0]["ReceiptTime"].ToString();
                                            if (ds.Tables["SuspendFlag"].Columns.Contains("value"))
                                                SuspendFlag_value = ds.Tables["SuspendFlag"].Rows[0]["value"].ToString();
                                        }
                                        else if (ds.Tables.Contains("VoidEvent"))
                                        {
                                            if (ds.Tables["VoidEvent"].Columns.Contains("EventSequenceID"))
                                                EventSequenceID = ds.Tables["VoidEvent"].Rows[0]["EventSequenceID"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("SaleEvent_Id"))
                                                SaleEvent_Id = ds.Tables["VoidEvent"].Rows[0]["SaleEvent_Id"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("CashierID"))
                                                CashierID = ds.Tables["VoidEvent"].Rows[0]["CashierID"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("RegisterID"))
                                                RegisterID = ds.Tables["VoidEvent"].Rows[0]["RegisterID"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("TillID"))
                                                TillID = ds.Tables["VoidEvent"].Rows[0]["TillID"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("TransactionID"))
                                                TransactionID = ds.Tables["VoidEvent"].Rows[0]["TransactionID"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("EventStartDate"))
                                                EventStartDate = ds.Tables["VoidEvent"].Rows[0]["EventStartDate"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("EventStartTime"))
                                                EventStartTime = ds.Tables["VoidEvent"].Rows[0]["EventStartTime"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("EventEndDate"))
                                                EventEndDate = ds.Tables["VoidEvent"].Rows[0]["EventEndDate"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("EventEndTime"))
                                                EventEndTime = ds.Tables["VoidEvent"].Rows[0]["EventEndTime"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("BusinessDate"))
                                                BusinessDate = ds.Tables["VoidEvent"].Rows[0]["BusinessDate"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("ReceiptDate"))
                                                ReceiptDate = ds.Tables["VoidEvent"].Rows[0]["ReceiptDate"].ToString();
                                            if (ds.Tables["VoidEvent"].Columns.Contains("ReceiptTime"))
                                                ReceiptTime = ds.Tables["VoidEvent"].Rows[0]["ReceiptTime"].ToString();
                                            if (ds.Tables["SuspendFlag"].Columns.Contains("value"))
                                                SuspendFlag_value = ds.Tables["SuspendFlag"].Rows[0]["value"].ToString();
                                            voidTrans = true;
                                        }
                                        else if (ds.Tables.Contains("FinancialEvent"))
                                        {
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("EventSequenceID"))
                                                EventSequenceID = ds.Tables["FinancialEvent"].Rows[0]["EventSequenceID"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("SaleEvent_Id"))
                                                SaleEvent_Id = ds.Tables["FinancialEvent"].Rows[0]["SaleEvent_Id"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("CashierID"))
                                                CashierID = ds.Tables["FinancialEvent"].Rows[0]["CashierID"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("RegisterID"))
                                                RegisterID = ds.Tables["FinancialEvent"].Rows[0]["RegisterID"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("TillID"))
                                                TillID = ds.Tables["FinancialEvent"].Rows[0]["TillID"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("TransactionID"))
                                                TransactionID = ds.Tables["FinancialEvent"].Rows[0]["TransactionID"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("EventStartDate"))
                                                EventStartDate = ds.Tables["FinancialEvent"].Rows[0]["EventStartDate"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("EventStartTime"))
                                                EventStartTime = ds.Tables["FinancialEvent"].Rows[0]["EventStartTime"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("EventEndDate"))
                                                EventEndDate = ds.Tables["FinancialEvent"].Rows[0]["EventEndDate"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("EventEndTime"))
                                                EventEndTime = ds.Tables["FinancialEvent"].Rows[0]["EventEndTime"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("BusinessDate"))
                                                BusinessDate = ds.Tables["FinancialEvent"].Rows[0]["BusinessDate"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("ReceiptDate"))
                                                ReceiptDate = ds.Tables["FinancialEvent"].Rows[0]["ReceiptDate"].ToString();
                                            if (ds.Tables["FinancialEvent"].Columns.Contains("ReceiptTime"))
                                                ReceiptTime = ds.Tables["FinancialEvent"].Rows[0]["ReceiptTime"].ToString();
                                            if (ds.Tables["SuspendFlag"].Columns.Contains("value"))
                                                SuspendFlag_value = ds.Tables["SuspendFlag"].Rows[0]["value"].ToString();
                                        }
                                        else if (ds.Tables.Contains("RefundEvent"))
                                        {
                                            if (ds.Tables["RefundEvent"].Columns.Contains("EventSequenceID"))
                                                EventSequenceID = ds.Tables["RefundEvent"].Rows[0]["EventSequenceID"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("SaleEvent_Id"))
                                                SaleEvent_Id = ds.Tables["RefundEvent"].Rows[0]["SaleEvent_Id"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("CashierID"))
                                                CashierID = ds.Tables["RefundEvent"].Rows[0]["CashierID"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("RegisterID"))
                                                RegisterID = ds.Tables["RefundEvent"].Rows[0]["RegisterID"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("TillID"))
                                                TillID = ds.Tables["RefundEvent"].Rows[0]["TillID"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("TransactionID"))
                                                TransactionID = ds.Tables["RefundEvent"].Rows[0]["TransactionID"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("EventStartDate"))
                                                EventStartDate = ds.Tables["RefundEvent"].Rows[0]["EventStartDate"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("EventStartTime"))
                                                EventStartTime = ds.Tables["RefundEvent"].Rows[0]["EventStartTime"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("EventEndDate"))
                                                EventEndDate = ds.Tables["RefundEvent"].Rows[0]["EventEndDate"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("EventEndTime"))
                                                EventEndTime = ds.Tables["RefundEvent"].Rows[0]["EventEndTime"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("BusinessDate"))
                                                BusinessDate = ds.Tables["RefundEvent"].Rows[0]["BusinessDate"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("ReceiptDate"))
                                                ReceiptDate = ds.Tables["RefundEvent"].Rows[0]["ReceiptDate"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("ReceiptTime"))
                                                ReceiptTime = ds.Tables["RefundEvent"].Rows[0]["ReceiptTime"].ToString();
                                            if (ds.Tables["RefundEvent"].Columns.Contains("value"))
                                                SuspendFlag_value = ds.Tables["RefundEvent"].Rows[0]["value"].ToString();
                                        }
                                        if (ds.Tables.Contains("TransactionSummary"))
                                        {
                                            if (ds.Tables["TransactionSummary"].Columns.Contains("TransactionTotalGrossAmount"))
                                                TransactionTotalGrossAmount = ds.Tables["TransactionSummary"].Rows[0]["TransactionTotalGrossAmount"].ToString();
                                            if (ds.Tables["TransactionSummary"].Columns.Contains("TransactionTotalNetAmount"))
                                                TransactionTotalNetAmount = ds.Tables["TransactionSummary"].Rows[0]["TransactionTotalNetAmount"].ToString();
                                            if (ds.Tables["TransactionSummary"].Columns.Contains("TransactionTotalTaxSalesAmount"))
                                                TransactionTotalTaxSalesAmount = ds.Tables["TransactionSummary"].Rows[0]["TransactionTotalTaxSalesAmount"].ToString();
                                            if (ds.Tables["TransactionSummary"].Columns.Contains("TransactionTotalTaxExemptAmount"))
                                                TransactionTotalTaxExemptAmount = ds.Tables["TransactionSummary"].Rows[0]["TransactionTotalTaxExemptAmount"].ToString();
                                            if (ds.Tables["TransactionSummary"].Columns.Contains("TransactionTotalTaxNetAmount"))
                                                TransactionTotalTaxNetAmount = ds.Tables["TransactionSummary"].Rows[0]["TransactionTotalTaxNetAmount"].ToString();
                                            if (ds.Tables["TransactionSummary"].Columns.Contains("TransactionSummary_Id"))

                                                TransactionSummary_Id = ds.Tables["TransactionSummary"].Rows[0]["TransactionSummary_Id"].ToString();


                                        }
                                        if (ds.Tables.Contains("TransactionTotalGrandAmount"))
                                        {
                                            if (ds.Tables["TransactionTotalGrandAmount"].Columns.Contains("TransactionTotalGrandAmount_Text"))
                                                TransactionTotalGrandAmount_Text = ds.Tables["TransactionTotalGrandAmount"].Rows[0]["TransactionTotalGrandAmount_Text"].ToString();
                                        }
                                        if (SuspendFlag_value == "yes")
                                        {
                                            pjrflg = 1;
                                            break;
                                        }
                                        if (dtTrans.Rows.Count == 0)
                                        {
                                            pjrflg = 0;
                                        }
                                        else
                                        {
                                            for (int k = 0; k < dtTrans.Rows.Count; k++)
                                            {
                                                if (dtTrans.Rows[k]["TransactionID"].ToString() == TransactionID && dtTrans.Rows[k]["EventEndTime"].ToString() == EventEndTime && dtTrans.Rows[k]["Storeid"].ToString() == storeId)
                                                {
                                                    pjrflg = 1;
                                                    break;
                                                }
                                                else if (status == "cancel")
                                                {
                                                    pjrflg = 1;
                                                    break;
                                                }
                                                else
                                                {
                                                    pjrflg = 0;
                                                }
                                            }
                                        }
                                        if (pjrflg != 1)
                                        {
                                            string Query = "insert into TransactionPJR VALUES('" + TransactionID + "','" + EventStartDate + "','" + EventStartTime + "','" + EventEndDate + "','" + EventEndTime + "','" + TransactionTotalGrossAmount + "','" + TransactionTotalNetAmount + "','" + TransactionTotalTaxSalesAmount + "','" + TransactionTotalTaxExemptAmount + "','" + TransactionTotalTaxNetAmount + "','" + TransactionTotalGrandAmount_Text + "','" + storeId + "','Sales Transaction','" + posId + "','" + voidTrans + "')";
                                            conn.Open();
                                            SqlCommand cmd = new SqlCommand();
                                            cmd = new SqlCommand(Query, conn);
                                            cmd.ExecuteNonQuery();
                                            conn.Close();
                                        }
                                    }
                                    if (ds.Tables.Contains("FuelLine"))
                                    {
                                        for (int i = 0; i < ds.Tables["FuelLine"].Rows.Count; i++)
                                        {
                                            if (ds.Tables["FuelLine"].Columns.Contains("FuelLine_id"))
                                            {
                                                FuelLine_id = ds.Tables["FuelLine"].Rows[i]["FuelLine_id"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("FuelGradeID"))
                                            {
                                                F_FuelGradeID = ds.Tables["FuelLine"].Rows[i]["FuelGradeID"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("FuelpositionID"))
                                            {
                                                F_FuelpostionID = ds.Tables["FuelLine"].Rows[i]["FuelpositionID"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("PriceTierCode"))
                                            {
                                                F_PriceTierCode = ds.Tables["FuelLine"].Rows[i]["PriceTierCode"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("TimeTierCode"))
                                            {
                                                F_TimeTierCode = ds.Tables["FuelLine"].Rows[i]["TimeTierCode"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("ServiceLevelCode"))
                                            {
                                                F_ServiceLevelCode = ds.Tables["FuelLine"].Rows[i]["ServiceLevelCode"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("Description"))
                                            {
                                                F_Description = ds.Tables["FuelLine"].Rows[i]["Description"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("ActualSalesPrice"))
                                            {
                                                F_ActualSalesPrice = ds.Tables["FuelLine"].Rows[i]["ActualSalesPrice"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("MerchandiseCode"))
                                            {
                                                F_MerchandiseCode = ds.Tables["FuelLine"].Rows[i]["MerchandiseCode"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("RegularSellPrice"))
                                            {
                                                F_RegularSellPrice = ds.Tables["FuelLine"].Rows[i]["RegularSellPrice"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("SalesQuantity"))
                                            {
                                                F_SalesQuantity = ds.Tables["FuelLine"].Rows[i]["SalesQuantity"].ToString();
                                            }
                                            if (ds.Tables["FuelLine"].Columns.Contains("SalesAmount"))
                                            {
                                                F_SalesAmount = ds.Tables["FuelLine"].Rows[i]["SalesAmount"].ToString();
                                            }
                                            if (ds.Tables["EntryMethod"].Columns.Contains("method"))
                                            {
                                                method = ds.Tables["EntryMethod"].Rows[i]["method"].ToString();
                                            }

                                            if (ds.Tables.Contains("Promotion"))
                                            {
                                                PromotionID = null;
                                                PromotionAmount = null;
                                                if (ds.Tables["Promotion"].Columns.Contains("FuelLine_Id"))
                                                {
                                                    for (int z = 0; z < ds.Tables["Promotion"].Rows.Count; z++)
                                                    {
                                                        if (ds.Tables["FuelLine"].Rows[i]["FuelLine_Id"].ToString() == ds.Tables["Promotion"].Rows[z]["FuelLine_Id"].ToString())
                                                        {
                                                            if (ds.Tables["Promotion"].Columns.Contains("PromotionID"))
                                                                PromotionID = ds.Tables["Promotion"].Rows[z]["PromotionID"].ToString();
                                                            else if (ds.Tables.Contains("PromotionID"))
                                                            {
                                                                PromotionID = ds.Tables["PromotionID"].Rows[z]["type"].ToString();
                                                            }
                                                            PromotionAmount = ds.Tables["Promotion"].Rows[z]["PromotionAmount"].ToString();
                                                        }
                                                    }
                                                }
                                            }
                                            if (ds.Tables.Contains("Discount"))
                                            {
                                                DiscountAmount = null;
                                                if (ds.Tables["Discount"].Columns.Contains("FuelLine_Id"))
                                                {
                                                    for (int z = 0; z < ds.Tables["Discount"].Rows.Count; z++)
                                                    {
                                                        if (ds.Tables["FuelLine"].Rows[i]["FuelLine_Id"].ToString() == ds.Tables["Discount"].Rows[z]["FuelLine_Id"].ToString())
                                                        {
                                                            DiscountAmount = ds.Tables["Discount"].Rows[i]["DiscountAmount"].ToString();
                                                        }
                                                    }
                                                }
                                            }

                                            if (ds.Tables["ItemTax"].Columns.Contains("TaxLevelID"))
                                            {
                                                for (int j = 0; j < ds.Tables["ItemTax"].Rows.Count; j++)
                                                {
                                                    if (ds.Tables["ItemTax"].Columns.Contains("FuelLine_id"))
                                                    {
                                                        string FuelLine_id1 = ds.Tables["ItemTax"].Rows[j]["FuelLine_id"].ToString();
                                                        if (FuelLine_id1 != "")
                                                        {
                                                            TaxLevelID = ds.Tables["ItemTax"].Rows[j]["TaxLevelID"].ToString();
                                                        }
                                                    }
                                                }
                                            }
                                            if (ds.Tables.Contains("MerchandiseCode"))
                                            {
                                                MerchandiseCode_id = ds.Tables["MerchandiseCode"].Rows[0]["MerchandiseCode_id"].ToString();
                                            }
                                            if (SuspendFlag_value == "yes")
                                            {
                                                pjrflg = 1;
                                                break;
                                            }
                                            if (dtFual.Rows.Count == 0)
                                            {
                                                pjrflg = 0;
                                            }
                                            else
                                            {
                                                for (int k = 0; k < dtFual.Rows.Count; k++)
                                                {
                                                    if (dtFual.Rows[k]["TransactionID"].ToString() == TransactionID && dtFual.Rows[k]["FuelLine_id"].ToString() == FuelLine_id && dtFual.Rows[k]["StoreId"].ToString() == storeId && dtFual.Rows[k]["EventEndDate"].ToString() == EventEndDate && dtFual.Rows[k]["POSId"].ToString() == posId)
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else if (status == "cancel")
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        pjrflg = 0;
                                                    }
                                                }
                                            }
                                            if (pjrflg != 1)
                                            {
                                                string Query = "insert into FuelPJR VALUES('" + TransactionID + "','normal','" + F_FuelGradeID + "','" + F_FuelpostionID + "','" + F_PriceTierCode + "','" + F_TimeTierCode + "','" + F_ServiceLevelCode + "','" + F_Description + "','" + method + "','" + F_ActualSalesPrice + "','" + F_MerchandiseCode + "','" + F_RegularSellPrice + "','" + F_SalesQuantity + "','" + F_SalesAmount + "','" + TaxLevelID + "','" + storeId + "','" + FuelLine_id + "','Fuel Sales','" + posId + "','" + EventEndDate + "','" + DiscountAmount + "','" + PromotionID + "','" + PromotionAmount + "')";
                                                conn.Open();
                                                SqlCommand cmd = new SqlCommand();
                                                cmd = new SqlCommand(Query, conn);
                                                cmd.ExecuteNonQuery();
                                                conn.Close();
                                            }
                                        }
                                    }
                                    if (ds.Tables.Contains("ItemLine"))
                                    {
                                        for (int i = 0; i < ds.Tables["ItemLine"].Rows.Count; i++)
                                        {
                                            if (ds.Tables["ItemLine"].Columns.Contains("ItemLine_Id"))
                                                ItemLine_Id = ds.Tables["ItemLine"].Rows[i]["ItemLine_Id"].ToString();
                                            if (ds.Tables["ItemLine"].Columns.Contains("Description"))
                                                Description = ds.Tables["ItemLine"].Rows[i]["Description"].ToString();
                                            if (ds.Tables["ItemLine"].Columns.Contains("ActualSalesPrice"))
                                                ActualSalesPrice = ds.Tables["ItemLine"].Rows[i]["ActualSalesPrice"].ToString();
                                            if (ds.Tables["ItemLine"].Columns.Contains("MerchandiseCode"))
                                                MerchandiseCode = ds.Tables["ItemLine"].Rows[i]["MerchandiseCode"].ToString();
                                            if (ds.Tables["ItemLine"].Columns.Contains("SellingUnits"))
                                                SellingUnits = ds.Tables["ItemLine"].Rows[i]["SellingUnits"].ToString();
                                            if (ds.Tables["ItemLine"].Columns.Contains("RegularSellPrice"))
                                                RegularSellPrice = ds.Tables["ItemLine"].Rows[i]["RegularSellPrice"].ToString();
                                            if (ds.Tables["ItemLine"].Columns.Contains("SalesQuantity"))
                                                SalesQuantity = ds.Tables["ItemLine"].Rows[i]["SalesQuantity"].ToString();
                                            if (ds.Tables["ItemCode"].Columns.Contains("ItemCode_Id"))
                                                ItemCode_Id = ds.Tables["ItemCode"].Rows[i]["ItemCode_Id"].ToString();
                                            if (ds.Tables["ItemCode"].Columns.Contains("POSCode"))
                                                POSCode = ds.Tables["ItemCode"].Rows[i]["POSCode"].ToString();
                                            if (ds.Tables["ItemLine"].Columns.Contains("SalesAmount"))
                                                SalesAmount = ds.Tables["ItemLine"].Rows[i]["SalesAmount"].ToString();
                                            if (ds.Tables["ItemTax"].Columns.Contains("TaxLevelID"))
                                                TaxLevelID = ds.Tables["ItemTax"].Rows[i]["TaxLevelID"].ToString();

                                            if (ds.Tables.Contains("Promotion"))
                                            {
                                                PromotionID = null;
                                                PromotionAmount = null;
                                                if (ds.Tables["Promotion"].Columns.Contains("ItemLine_Id"))
                                                {
                                                    for (int z = 0; z < ds.Tables["Promotion"].Rows.Count; z++)
                                                    {
                                                        if (ds.Tables["ItemLine"].Rows[i]["ItemLine_Id"].ToString() == ds.Tables["Promotion"].Rows[z]["ItemLine_Id"].ToString())
                                                        {
                                                            if (ds.Tables["Promotion"].Columns.Contains("PromotionID"))
                                                                PromotionID = ds.Tables["Promotion"].Rows[z]["PromotionID"].ToString();
                                                            else if (ds.Tables.Contains("PromotionID"))
                                                            {
                                                                PromotionID = ds.Tables["PromotionID"].Rows[z]["type"].ToString();
                                                            }
                                                            PromotionAmount = ds.Tables["Promotion"].Rows[z]["PromotionAmount"].ToString();
                                                        }
                                                    }
                                                }
                                            }
                                            if (ds.Tables.Contains("Discount"))
                                            {
                                                DiscountAmount = null;
                                                if (ds.Tables["Discount"].Columns.Contains("ItemLine_Id"))
                                                {
                                                    for (int z = 0; z < ds.Tables["Discount"].Rows.Count; z++)
                                                    {
                                                        if (ds.Tables["ItemLine"].Rows[i]["ItemLine_Id"].ToString() == ds.Tables["Discount"].Rows[z]["ItemLine_Id"].ToString())
                                                        {
                                                            DiscountAmount = ds.Tables["Discount"].Rows[i]["DiscountAmount"].ToString();
                                                        }
                                                    }
                                                }
                                            }

                                            if (ds.Tables["ItemTax"].Columns.Contains("TaxLevelID"))
                                            {
                                                for (int j = 0; j < ds.Tables["ItemTax"].Rows.Count; j++)
                                                {
                                                    if (ds.Tables["ItemTax"].Columns.Contains("ItemLine_Id"))
                                                    {
                                                        string FuelLine_id1 = ds.Tables["ItemTax"].Rows[j]["ItemLine_Id"].ToString();
                                                        if (FuelLine_id1 != "")
                                                        {
                                                            TaxLevelID = ds.Tables["ItemTax"].Rows[j]["TaxLevelID"].ToString();
                                                        }
                                                    }
                                                }
                                            }
                                            if (ds.Tables["EntryMethod"].Columns.Contains("method"))
                                                method = ds.Tables["EntryMethod"].Rows[i]["method"].ToString();
                                            if (SuspendFlag_value == "yes")
                                            {
                                                pjrflg = 1;
                                                break;
                                            }
                                            if (dtitem.Rows.Count == 0)
                                            {
                                                pjrflg = 0;
                                            }
                                            else
                                            {
                                                for (int k = 0; k < dtitem.Rows.Count; k++)
                                                {
                                                    if (dtitem.Rows[k]["TransactionID"].ToString() == TransactionID && dtitem.Rows[k]["ItemLine_Id"].ToString() == ItemLine_Id && dtitem.Rows[k]["Storeid"].ToString() == storeId && dtitem.Rows[k]["EventEndDate"].ToString() == EventEndDate && dtitem.Rows[k]["POSId"].ToString() == posId)
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else if (status == "cancel")
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        pjrflg = 0;
                                                    }
                                                }
                                            }
                                            if (pjrflg != 1)
                                            {
                                                if (Description != null)
                                                {
                                                    Description = Description.Replace("'", " ");
                                                }
                                                string Query = "insert into ItemPJR VALUES('" + TransactionID + "','normal','" + format + "','" + POSCode + "','" + Description + "','" + method + "','" + ActualSalesPrice + "','" + MerchandiseCode + "','" + SellingUnits + "','" + RegularSellPrice + "','" + SalesQuantity + "','" + SalesAmount + "','" + TaxLevelID + "','" + storeId + "','" + ItemLine_Id + "','Items Sales','" + posId + "','" + EventEndDate + "','" + PromotionID + "','" + PromotionAmount + "','" + DiscountAmount + "')";
                                                conn.Open();
                                                SqlCommand cmd = new SqlCommand();
                                                cmd = new SqlCommand(Query, conn);
                                                cmd.ExecuteNonQuery();
                                                conn.Close();
                                            }
                                        }
                                    }
                                    if (ds.Tables.Contains("MerchandiseCodeLine"))
                                    {
                                        for (int i = 0; i < ds.Tables["MerchandiseCodeLine"].Rows.Count; i++)
                                        {
                                            if (ds.Tables["MerchandiseCodeLine"].Columns.Contains("Description"))
                                                M_Description = ds.Tables["MerchandiseCodeLine"].Rows[i]["Description"].ToString();
                                            if (ds.Tables["MerchandiseCodeLine"].Columns.Contains("ActualSalesPrice"))
                                                M_ActualSalesPrice = ds.Tables["MerchandiseCodeLine"].Rows[i]["ActualSalesPrice"].ToString();
                                            if (ds.Tables["MerchandiseCodeLine"].Columns.Contains("MerchandiseCode"))
                                                M_MerchandiseCode = ds.Tables["MerchandiseCodeLine"].Rows[i]["MerchandiseCode"].ToString();
                                            if (ds.Tables["MerchandiseCodeLine"].Columns.Contains("RegularSellPrice"))
                                                M_RegularSellPrice = ds.Tables["MerchandiseCodeLine"].Rows[i]["RegularSellPrice"].ToString();
                                            if (ds.Tables["MerchandiseCodeLine"].Columns.Contains("SalesQuantity"))
                                                M_SalesQuantity = ds.Tables["MerchandiseCodeLine"].Rows[i]["SalesQuantity"].ToString();
                                            if (ds.Tables["MerchandiseCodeLine"].Columns.Contains("SalesAmount"))
                                                M_SalesAmount = ds.Tables["MerchandiseCodeLine"].Rows[i]["SalesAmount"].ToString();

                                            if (ds.Tables.Contains("Promotion"))
                                            {
                                                PromotionID = null;
                                                PromotionAmount = null;
                                                if (ds.Tables["Promotion"].Columns.Contains("MerchandiseCodeLine_Id"))
                                                {
                                                    for (int z = 0; z < ds.Tables["Promotion"].Rows.Count; z++)
                                                    {
                                                        if (ds.Tables["MerchandiseCodeLine"].Rows[i]["MerchandiseCodeLine_Id"].ToString() == ds.Tables["Promotion"].Rows[z]["MerchandiseCodeLine_Id"].ToString())
                                                        {
                                                            if (ds.Tables["Promotion"].Columns.Contains("PromotionID"))
                                                                PromotionID = ds.Tables["Promotion"].Rows[z]["PromotionID"].ToString();
                                                            else if (ds.Tables.Contains("PromotionID"))
                                                            {
                                                                PromotionID = ds.Tables["PromotionID"].Rows[z]["type"].ToString();
                                                            }
                                                            PromotionAmount = ds.Tables["Promotion"].Rows[z]["PromotionAmount"].ToString();
                                                        }
                                                    }
                                                }
                                            }

                                            if (ds.Tables.Contains("Discount"))
                                            {
                                                DiscountAmount = null;
                                                if (ds.Tables["Discount"].Columns.Contains("MerchandiseCodeLine_Id"))
                                                {
                                                    for (int z = 0; z < ds.Tables["Discount"].Rows.Count; z++)
                                                    {
                                                        if (ds.Tables["MerchandiseCodeLine"].Rows[i]["MerchandiseCodeLine_Id"].ToString() == ds.Tables["Discount"].Rows[z]["MerchandiseCodeLine_Id"].ToString())
                                                        {
                                                            DiscountAmount = ds.Tables["Discount"].Rows[i]["DiscountAmount"].ToString();
                                                        }
                                                    }
                                                }
                                            }

                                            if (ds.Tables.Contains("ItemTax"))
                                            {
                                                if (ds.Tables["ItemTax"].Columns.Contains("TaxLevelID"))
                                                {
                                                    TaxLevelID = ds.Tables["ItemTax"].Rows[i]["TaxLevelID"].ToString();
                                                }
                                            }
                                            if (SuspendFlag_value == "yes")
                                            {
                                                pjrflg = 1;
                                                break;
                                            }
                                            if (dtmer.Rows.Count == 0)
                                            {
                                                pjrflg = 0;
                                            }
                                            else
                                            {
                                                for (int k = 0; k < dtmer.Rows.Count; k++)
                                                {
                                                    if (dtmer.Rows[k]["TransactionID"].ToString() == TransactionID && dtmer.Rows[k]["MerchandiseCode"].ToString() == M_MerchandiseCode && dtmer.Rows[k]["Storeid"].ToString() == storeId && dtmer.Rows[k]["EventEndDate"].ToString() == EventEndDate && dtmer.Rows[k]["POSId"].ToString() == posId)
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else if (status == "cancel")
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        pjrflg = 0;
                                                    }
                                                }
                                            }
                                            if (pjrflg != 1)
                                            {
                                                string Query = "insert into MerchandisePJR VALUES('" + TransactionID + "','normal','" + M_Description + "','" + M_MerchandiseCode + "','" + M_ActualSalesPrice + "','" + M_SalesQuantity + "','" + M_RegularSellPrice + "','" + M_SalesAmount + "','" + TaxLevelID + "','" + storeId + "','Merchandise Sales','" + posId + "','" + EventEndDate + "','" + DiscountAmount + "','" + PromotionID + "','" + PromotionAmount + "')";
                                                conn.Open();
                                                SqlCommand cmd = new SqlCommand();
                                                cmd = new SqlCommand(Query, conn);
                                                cmd.ExecuteNonQuery();
                                                conn.Close();
                                            }
                                        }
                                    }
                                    if (ds.Tables.Contains("TenderInfo"))
                                    {
                                        for (int i = 0; i < ds.Tables["TenderInfo"].Rows.Count; i++)
                                        {
                                            if (ds.Tables["TenderInfo"].Columns.Contains("TenderAmount"))
                                            {
                                                if (ds.Tables.Contains("ChangeFlag"))
                                                {
                                                    if (ds.Tables["ChangeFlag"].Rows[i]["value"].ToString() == "yes")
                                                    {
                                                        TenderAmount = (Convert.ToDecimal(ds.Tables["TenderInfo"].Rows[i]["TenderAmount"]) * -1).ToString();
                                                    }
                                                    else
                                                    {
                                                        TenderAmount = ds.Tables["TenderInfo"].Rows[i]["TenderAmount"].ToString();
                                                    }
                                                }
                                            }
                                            if (ds.Tables["TenderInfo"].Columns.Contains("TenderInfo_Id"))
                                                TenderInfo_Id = ds.Tables["TenderInfo"].Rows[i]["TenderInfo_Id"].ToString();
                                            if (ds.Tables.Contains("Tender"))
                                            {
                                                if (ds.Tables["Tender"].Columns.Contains("TenderCode"))
                                                    TenderCode = ds.Tables["Tender"].Rows[i]["TenderCode"].ToString();
                                                if (ds.Tables["Tender"].Columns.Contains("TenderSubCode"))
                                                    TenderSubCode = ds.Tables["Tender"].Rows[i]["TenderSubCode"].ToString();
                                            }
                                            if (ds.Tables.Contains("AccountInfo"))
                                            {
                                                if (ds.Tables["AccountInfo"].Columns.Contains("AccountName"))
                                                    AccountName = ds.Tables["AccountInfo"].Rows[0]["AccountName"].ToString();
                                            }
                                            if (SuspendFlag_value == "yes")
                                            {
                                                pjrflg = 1;
                                                break;
                                            }
                                            if (dttend.Rows.Count == 0)
                                            {
                                                pjrflg = 0;
                                            }
                                            else
                                            {
                                                for (int k = 0; k < dttend.Rows.Count; k++)
                                                {
                                                    if (dttend.Rows[k]["TransactionID"].ToString() == TransactionID && dttend.Rows[k]["TenderInfo_Id"].ToString() == TenderInfo_Id && dttend.Rows[k]["Storeid"].ToString() == storeId && dttend.Rows[k]["EventEndDate"].ToString() == EventEndDate && dttend.Rows[k]["POSId"].ToString() == posId)
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else if (status == "cancel")
                                                    {
                                                        pjrflg = 1;
                                                        break;
                                                    }
                                                    else
                                                    {
                                                        pjrflg = 0;
                                                    }
                                                }
                                            }
                                            if (pjrflg != 1)
                                            {
                                                if (AccountName != null)
                                                {
                                                    AccountName = AccountName.Replace("'", " ");
                                                }
                                                string Query = "insert into TenderPJR VALUES('" + TransactionID + "','normal','" + TenderCode + "','" + TenderSubCode + "','" + TenderAmount + "','" + value + "','" + storeId + "','" + TenderInfo_Id + "','Tender Sales','" + posId + "','" + EventEndDate + "','" + AccountName + "')";
                                                conn.Open();
                                                SqlCommand cmd = new SqlCommand();
                                                cmd = new SqlCommand(Query, conn);
                                                cmd.ExecuteNonQuery();
                                                conn.Close();
                                            }
                                        }
                                    }
                                }
                                string copypath = Path.GetDirectoryName(Application.StartupPath);
                                GrantAccess(copypath);
                                string copyPath = copypath + "\\CopyFiles\\";
                                if (!Directory.Exists(copyPath))
                                {
                                    Directory.CreateDirectory(copyPath);
                                }
                                string copyPathDate = copyPath + folderName + "\\";
                                if (!Directory.Exists(copyPathDate))
                                {
                                    Directory.CreateDirectory(copyPathDate);
                                }

                                File.Copy(XMlFile, copyPathDate + Path.GetFileName(XMlFile), true);
                                File.Delete(XMlFile);
                                if (str == "MCM" || str == "MSM" || str == "PJR")
                                {
                                    string deleteFolder = copyPath + Convert.ToDateTime(folderName).AddMonths(-1).ToString("yyyy-MM-dd");
                                    if (Directory.Exists(deleteFolder))
                                    {
                                        var dir = new DirectoryInfo(deleteFolder);
                                        dir.Attributes = dir.Attributes & ~FileAttributes.ReadOnly;
                                        dir.Delete(true);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                SendErrorToText(ex, errorFileName);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SendErrorToText(ex, errorFileName);
            }
        }
        private static void GrantAccess(string file)
        {
            bool exists = System.IO.Directory.Exists(file);
            if (!exists)
            {
                DirectoryInfo di = System.IO.Directory.CreateDirectory(file);
                Console.WriteLine("The Folder is created Sucessfully");
            }
            else
            {
                Console.WriteLine("The Folder already exists");
            }
            DirectoryInfo dInfo = new DirectoryInfo(file);
            DirectorySecurity dSecurity = dInfo.GetAccessControl();
            dSecurity.AddAccessRule(new FileSystemAccessRule(new SecurityIdentifier(WellKnownSidType.WorldSid, null), FileSystemRights.FullControl, InheritanceFlags.ObjectInherit | InheritanceFlags.ContainerInherit, PropagationFlags.NoPropagateInherit, AccessControlType.Allow));
            dInfo.SetAccessControl(dSecurity);

        }
        private static String ErrorlineNo, Errormsg, extype, ErrorLocation, exurl, hostIp;

        private void Form1_Load(object sender, EventArgs e)
        {

        }

        public static void SendErrorToText(Exception ex, string errorFileName)
        {
            var line = Environment.NewLine + Environment.NewLine;
            ErrorlineNo = ex.StackTrace.Substring(ex.StackTrace.Length - 7, 7);
            Errormsg = ex.GetType().Name.ToString();
            extype = ex.GetType().ToString();

            ErrorLocation = ex.Message.ToString();
            try
            {
                string filepath = Path.GetDirectoryName(Application.StartupPath);
                GrantAccess(filepath);
                string errorpath = filepath + "\\ErrorFiles\\";
                if (!Directory.Exists(errorpath))
                {
                    Directory.CreateDirectory(errorpath);
                }
                if (!Directory.Exists(filepath))
                {
                    Directory.CreateDirectory(filepath);
                }
                filepath = filepath + "\\log.txt";   //Text File Name
                if (!File.Exists(filepath))
                {
                    File.Create(filepath).Dispose();
                }
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    string error = "Log Written Date:" + " " + DateTime.Now.ToString() + line + "Error Line No :" + " " + ErrorlineNo + line + "Error Message:" + " " + Errormsg + line + "Exception Type:" + " " + extype + line + "Error Location :" + " " + ErrorLocation + line + " Error Page Url:" + " " + exurl + line + "User Host IP:" + " " + hostIp + line;
                    sw.WriteLine("-----------Exception Details on " + " " + DateTime.Now.ToString() + "-----------------");
                    sw.WriteLine("-------------------------------------------------------------------------------------");
                    sw.WriteLine(line);
                    sw.WriteLine(error);
                    sw.WriteLine("--------------------------------*End*------------------------------------------");
                    sw.WriteLine(line);
                    sw.Flush();
                    sw.Close();
                }
                if (ErrorLocation == "Root element is missing.")
                {
                    File.Copy(errorFileName, errorpath + Path.GetFileName(errorFileName), true);
                    File.Delete(errorFileName);
                }
                Application.Restart();
            }
            catch (Exception)
            {
                Application.Restart();
            }
        }

        private void ScanDataAltria()
        {
            string errorFileName = "";
            try
            {
                //var startdate = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow.AddDays(-7), TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("MM/dd/yyyy").Replace("-", "/");
                //var enddate = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("MM/dd/yyyy").Replace("-", "/");

                DateTime nextWednesday = DateTime.Now.AddDays(-1);
                while (nextWednesday.DayOfWeek != DayOfWeek.Saturday)
                    nextWednesday = nextWednesday.AddDays(-1);
                DateTime lastWednesday = DateTime.Now.AddDays(-8);
                while (lastWednesday.DayOfWeek != DayOfWeek.Sunday)
                    lastWednesday = lastWednesday.AddDays(-1);

                var startdate = lastWednesday.ToString("MM/dd/yyyy").Replace("-", "/");
                var enddate = nextWednesday.ToString("MM/dd/yyyy").Replace("-", "/");

                if (startdate != null && enddate != null && storeId != null && posId != null)
                {
                    DataSet ds = new DataSet("ScanData");
                    using (SqlConnection con = new SqlConnection(conString))
                    {
                        using (SqlCommand cmd2 = new SqlCommand("[dbo].[sp_ScanData]", con))
                        {
                            con.Open();
                            cmd2.CommandType = CommandType.StoredProcedure;
                            cmd2.Parameters.Add("@startdate", SqlDbType.VarChar).Value = startdate;
                            cmd2.Parameters.Add("@enddate", SqlDbType.VarChar).Value = enddate;
                            cmd2.Parameters.Add("@storeId", SqlDbType.VarChar).Value = storeId;
                            cmd2.Parameters.Add("@type", SqlDbType.VarChar).Value = "Altria";
                            SqlDataAdapter adp = new SqlDataAdapter();
                            adp.SelectCommand = cmd2;
                            adp.Fill(ds);
                            con.Close();
                        }
                    }
                    var rowCount = ds.Tables[0].Rows[0].ItemArray[0];

                    if (int.Parse(rowCount.ToString()) != 0)
                    {
                        DataTable dtScandataSetting = new DataTable();
                        using (SqlConnection conn = new SqlConnection(conString))
                        {
                            using (SqlCommand cmd = new SqlCommand("select * from ScanDataSetting where StoreId=@StoreId and name=@type", conn))
                            {
                                conn.Open();
                                cmd.Parameters.Add("@StoreId", SqlDbType.VarChar).Value = storeId;
                                cmd.Parameters.Add("@type", SqlDbType.VarChar).Value = "Altria";
                                SqlDataAdapter adp = new SqlDataAdapter();
                                adp.SelectCommand = cmd;
                                adp.Fill(dtScandataSetting);
                                conn.Close();
                            }
                        }

                        DataTable dtFile = new DataTable();
                        using (SqlConnection conFile = new SqlConnection(conString))
                        {
                            using (SqlCommand cmdFile = new SqlCommand("[dbo].[sp_ScanDataFile]", conFile))
                            {
                                conFile.Open();
                                cmdFile.CommandType = CommandType.StoredProcedure;
                                cmdFile.Parameters.Add("@startdate", SqlDbType.VarChar).Value = startdate;
                                cmdFile.Parameters.Add("@enddate", SqlDbType.VarChar).Value = enddate;
                                cmdFile.Parameters.Add("@storeId", SqlDbType.VarChar).Value = storeId;
                                cmdFile.Parameters.Add("@type", SqlDbType.VarChar).Value = "Altria";
                                SqlDataAdapter adp = new SqlDataAdapter();
                                adp.SelectCommand = cmdFile;
                                adp.Fill(dtFile);
                                conFile.Close();
                            }
                        }

                        if (dtFile.Rows.Count != 0)
                        {
                            string end_date = nextWednesday.ToString("yyyy/MM/dd").Replace("-", "");
                            string fileName = (dtFile.Rows[0].ItemArray[6].ToString() + end_date).Replace(" ", "") + ".txt";
                            //string fileNamePath = Path.GetFullPath(fileName);

                            string fileNamePath = Path.GetDirectoryName(Application.StartupPath); //Path.GetFullPath(fileName);
                            string copyPath = fileNamePath + "\\ScanData\\";
                            if (!Directory.Exists(copyPath))
                            {
                                Directory.CreateDirectory(copyPath);
                            }
                            copyPath = copyPath + fileName;

                            StreamWriter writer = new StreamWriter(copyPath);


                            int sumQuantity = Convert.ToInt32(dtFile.Compute("SUM(Quantity)", string.Empty));
                            decimal sumFinalSalesPrice = Convert.ToDecimal(dtFile.Compute("SUM(FinalSalesPrice)", string.Empty));

                            writer.WriteLine(dtFile.Rows.Count + "|" + sumQuantity + "|" + sumFinalSalesPrice + "|" + "PSPCStore");
                            foreach (DataRow row in dtFile.AsEnumerable())
                            {
                                //writer.WriteLine(string.Join("|", row.ItemArray.Select(x => x.ToString())) + "|");
                                writer.WriteLine(string.Join("|", row.ItemArray.Select(x => x.ToString())));
                            }
                            string ftppath = dtScandataSetting.Rows[0]["Server"].ToString();

                            writer.Close();
                            string from = fileNamePath;
                            //string to = "ftp://APIMobile.vivo-soft.com/" + fileName.Replace(".txt", "");
                            string to = "sFTP://" + ftppath + "/" + fileName.Replace(".txt", "");
                            //string user = "mobileapi";
                            //string pass = "09#Prem#24";
                            string user = dtScandataSetting.Rows[0]["UserName"].ToString();
                            string pass = dtScandataSetting.Rows[0]["Password"].ToString();

                            //WebClient client = new WebClient();
                            //client.Credentials = new NetworkCredential(user, pass);
                            //client.UploadFile(to, WebRequestMethods.Ftp.UploadFile, from);

                            SftpClient sftpClient = new SftpClient(ftppath, user, pass);
                            sftpClient.Connect();
                            FileStream fs = new FileStream(from, FileMode.Open);
                            sftpClient.UploadFile(fs, Path.GetFileName(from));
                            sftpClient.Dispose();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SendErrorToText(ex, errorFileName);
            }
        }

        private void ScanDataRJR()
        {
            string errorFileName = "";
            try
            {
                var startdate = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow.AddDays(-7), TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("MM/dd/yyyy").Replace("-", "/");
                var enddate = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("MM/dd/yyyy").Replace("-", "/");

                if (startdate != null && enddate != null && storeId != null && posId != null)
                {
                    DataSet ds = new DataSet("ScanData");
                    using (SqlConnection con = new SqlConnection(conString))
                    {
                        using (SqlCommand cmd2 = new SqlCommand("[dbo].[sp_ScanData]", con))
                        {
                            con.Open();
                            cmd2.CommandType = CommandType.StoredProcedure;
                            cmd2.Parameters.Add("@startdate", SqlDbType.VarChar).Value = startdate;
                            cmd2.Parameters.Add("@enddate", SqlDbType.VarChar).Value = enddate;
                            cmd2.Parameters.Add("@storeId", SqlDbType.VarChar).Value = storeId;
                            cmd2.Parameters.Add("@type", SqlDbType.VarChar).Value = "RJR";
                            SqlDataAdapter adp = new SqlDataAdapter();
                            adp.SelectCommand = cmd2;
                            adp.Fill(ds);
                            con.Close();
                        }
                    }
                    var rowCount = ds.Tables[0].Rows[0].ItemArray[0];

                    if (int.Parse(rowCount.ToString()) != 0)
                    {

                        SqlConnection conn = new SqlConnection(conString);
                        string RJR = "RJR";
                        DataTable dtScandataSetting = new DataTable();
                        string query1 = "select * from ScanDataSetting where name='" + RJR + "' and StoreId=" + storeId;
                        SqlCommand cmd = new SqlCommand(query1, conn);
                        conn.Open();
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        da.Fill(dtScandataSetting);
                        conn.Close();
                        da.Dispose();

                        DataSet dsFile = new DataSet();
                        using (SqlConnection conFile = new SqlConnection(conString))
                        {
                            using (SqlCommand cmdFile = new SqlCommand("[dbo].[sp_ScanDataFile]", conFile))
                            {
                                conFile.Open();
                                cmdFile.CommandType = CommandType.StoredProcedure;
                                cmdFile.Parameters.Add("@startdate", SqlDbType.VarChar).Value = startdate;
                                cmdFile.Parameters.Add("@enddate", SqlDbType.VarChar).Value = enddate;
                                cmdFile.Parameters.Add("@storeId", SqlDbType.VarChar).Value = storeId;
                                cmdFile.Parameters.Add("@type", SqlDbType.VarChar).Value = "RJR";
                                SqlDataAdapter adp = new SqlDataAdapter();
                                adp.SelectCommand = cmdFile;
                                adp.Fill(dsFile);
                                conFile.Close();
                            }
                        }
                        if (dsFile.Tables.Count != 0)
                        {
                            string end_date = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("yyyy/MM/dd").Replace("-", "");
                            string fileName = (dsFile.Tables[0].Rows[0].ItemArray[0].ToString() + end_date).Replace(" ", "") + ".json";
                            var jsonStr = JsonConvert.SerializeObject(dsFile.Tables[0]);
                            string path = Path.GetFullPath(@"Content");
                            string ftppath = dtScandataSetting.Rows[0]["FTPPath"].ToString();
                            System.IO.File.WriteAllText(path + fileName, jsonStr);



                            string from = path + fileName;
                            string to = ftppath + "/" + fileName.Replace(".json", "");
                            string user = dtScandataSetting.Rows[0]["UserName"].ToString();
                            string pass = dtScandataSetting.Rows[0]["Password"].ToString();

                            WebClient client = new WebClient();
                            client.Credentials = new NetworkCredential(user, pass);
                            client.UploadFile(to, WebRequestMethods.Ftp.UploadFile, from);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SendErrorToText(ex, errorFileName);
            }
        }
    }
}

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
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Security.Policy;
using System.Diagnostics;
using System.IO.Compression;

namespace Connecter
{
    public partial class Form1 : Form
    {
        string conString = "Server=" + ConfigurationManager.AppSettings["ServerName"].ToString() + "; Database=" + ConfigurationManager.AppSettings["DBName"].ToString() + "; User Id=test; Password=Prem#12681#; Trusted_Connection=False; MultipleActiveResultSets=true";
        string storeId = AppCommon.StoreId(ConfigurationManager.AppSettings.Get("Key"));
        string posId = AppCommon.POSId(ConfigurationManager.AppSettings.Get("Key"));
        string sourcePath = ConfigurationManager.AppSettings.Get("SourcePath");
        string outPath = ConfigurationManager.AppSettings.Get("OutPath");
        string basePath = AppDomain.CurrentDomain.BaseDirectory;
        bool isSend = false;

        string url = "";
        string Version = "";
        string ZipFileName = "";
        string time = "";
        string Day = "";
        bool isDone = false;
        public Form1()
        {
            Task task = new Task(() =>
            {
                while (true)
                {
                    url = ConfigurationManager.AppSettings.Get("URL");
                    time = DateTime.Now.ToString("HH");
                    Day = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("dddd");

                    SqlConnection conn = new SqlConnection(conString);
                    string queryString = string.Format("Select POSType from POSType Where StoreId={0}", storeId);
                    SqlDataAdapter adapter = new SqlDataAdapter(queryString, conn);
                    DataTable dt = new DataTable();
                    adapter.Fill(dt);
                    foreach (DataRow item in dt.Rows)
                    {
                        //if (item["POSType"].ToString() == "PSPCStore")
                        //{
                        //    SqlConnection connVersion = new SqlConnection(conString);
                        //    string queryStringVersion = string.Format("Select * from  FileDetails");
                        //    SqlDataAdapter adapterVersion = new SqlDataAdapter(queryStringVersion, connVersion);
                        //    DataTable dtVersion = new DataTable();
                        //    adapterVersion.Fill(dtVersion);
                        //    if (dtVersion.Rows.Count != 0)
                        //    {
                        //        Version = dtVersion.Rows[0]["Version"].ToString();
                        //        ZipFileName = dtVersion.Rows[0]["Name"].ToString();
                        //    }
                        //    var ver = "";
                        //    var basePathVersion = basePath + "POS";
                        //    if (!Directory.Exists(basePathVersion))
                        //        Directory.CreateDirectory(basePathVersion);
                        //    if (File.Exists(basePathVersion + "//PSPCStore.exe"))
                        //    {
                        //        FileVersionInfo myFileVersionInfo = FileVersionInfo.GetVersionInfo(basePathVersion + "//PSPCStore.exe");
                        //        ver = myFileVersionInfo.FileVersion;
                        //    }
                        //    else
                        //    {
                        //        DownLoadZip();
                        //        ExtracZip();
                        //    }

                        //    if (Convert.ToInt32(time) == 1)
                        //    {
                        //        if (ver != Version)
                        //        {
                        //            DownLoadZip();
                        //            ExtracZip();
                        //        }
                        //    }
                        //    if (url != null)
                        //    {
                        //         copyFile();
                        //    }
                        //}
                        //else 
                        if (item["POSType"].ToString() == "PassPort")
                        {
                            PassportWrite();
                            PassportRead();
                        }
                        else if (item["POSType"].ToString() == "Commonder")
                        {
                            VerifoneRead();
                        }
                    }
                    try
                    {
                        if (Day == "Tuesday" && isDone == false)
                        {
                            // SqlConnection conn = new SqlConnection(conString);
                            string query = string.Format("Select isScanData from store Where StoreId={0}", storeId);
                            SqlDataAdapter adapters = new SqlDataAdapter(query, conn);
                            DataTable dts = new DataTable();
                            adapters.Fill(dts);

                            if (Convert.ToBoolean(dts.Rows[0].ItemArray[0]) == true)
                            {
                                RJR();
                                Altria();
                                isDone = true;
                            }
                        }
                        if (Day != "Tuesday")
                        {
                            isDone = false;
                        }
                    }
                    catch (Exception e)
                    {
                        SendErrorToText(e, "Scandata");
                    }
                    //}
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
        public void PassportRead()
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
                                                string Query = "insert into MCM (BeginDate,BeginTime,EndDate,EndTime,MerchandiseCode,MerchandiseCodeDescription,MCMDetail_Id,DiscountAmount,DiscountCount,PromotionAmount,PromotionCount,RefundAmount,RefundCount,SalesQuantity,SalesAmount,TransactionCount,OpenDepartmentSalesAmount,OpenDepartmentTransactionCount,StoreId,POSId)  " +
                                                    "VALUES('" + BeginDate + "','" + BeginTime + "','" + EndDate + "','" + EndTime + "','" + MerchandiseCode + "','" + MerchandiseCodeDescription + "','" + MCMDetail_Id + "','" + DiscountAmount + "','" + DiscountCount + "','" + PromotionAmount + "','" + PromotionCount + "','" + RefundAmount + "','" + RefundCount + "','" + SalesQuantity + "','" + SalesAmount + "','" + TransactionCount + "','" + OpenDepartmentSalesAmount + "','" + OpenDepartmentTransactionCount + "','" + storeId + "','" + posId + "')";
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
                                                string Query = "insert into MSM (BeginDate,BeginTime,EndDate,EndTime,MSMDetail_Id,MiscellaneousSummaryCode,MiscellaneousSummarySubCode,MiscellaneousSummarySubCodeModifier,MSMSalesTotals_Id,MiscellaneousSummaryAmount,MiscellaneousSummaryCount,TenderCode,TenderSubCode,StoreId,POSId)   " +
                                                    "VALUES('" + BeginDate + "','" + BeginTime + "','" + EndDate + "','" + EndTime + "','" + MSMDetail_Id + "','" + MiscellaneousSummaryCode + "','" + MiscellaneousSummarySubCode + "','" + MiscellaneousSummarySubCodeModifier + "','" + MSMSalesTotals_Id + "','" + MiscellaneousSummaryAmount + "','" + MiscellaneousSummaryCount + "','" + TenderCode + "','" + TenderSubCode + "','" + storeId + "','" + posId + "')";
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
                                    string CustomerLoyaltyId = null;
                                    string LoyaltyProgramName = null;
                                    string LoyaltyEntryMethod = null;
                                    string LoyaltyDiscountAmount = null;

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
                                            string Query = "insert into TransactionPJR  (TransactionID,EventStartDate,EventStartTime,EventEndDate,EventEndTime,TransactionTotalGrossAmount,TransactionTotalNetAmount,TransactionTotalSalesAmount,TransactionTotalTaxExemptAmount,TransactionTotalTaxNetAmount,TransactionTotalGrandAmount,StoreId,VoucherType,POSId,void)  " +
                                                "VALUES('" + TransactionID + "','" + EventStartDate + "','" + EventStartTime + "','" + EventEndDate + "','" + EventEndTime + "','" + TransactionTotalGrossAmount + "','" + TransactionTotalNetAmount + "','" + TransactionTotalTaxSalesAmount + "','" + TransactionTotalTaxExemptAmount + "','" + TransactionTotalTaxNetAmount + "','" + TransactionTotalGrandAmount_Text + "','" + storeId + "','Sales Transaction','" + posId + "','" + voidTrans + "')";
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
                                                string Query = "insert into FuelPJR (TransactionId,Status,FuelGradeId,FuelPossitionId,PriceTierCode,TimeTierCode,ServiceLevelCode,Description,Method,ActualSalesPrice,MerchandiseCode,RegularSellPrice,SalesQuantity,SalesAmount,TaxLevelld,StoreId,FuelLine_id,VoucherType,POSId,EventEndDate,DiscountAmount,PromotionID,PromotionAmount)  " +
                                                    "VALUES('" + TransactionID + "','normal','" + F_FuelGradeID + "','" + F_FuelpostionID + "','" + F_PriceTierCode + "','" + F_TimeTierCode + "','" + F_ServiceLevelCode + "','" + F_Description + "','" + method + "','" + F_ActualSalesPrice + "','" + F_MerchandiseCode + "','" + F_RegularSellPrice + "','" + F_SalesQuantity + "','" + F_SalesAmount + "','" + TaxLevelID + "','" + storeId + "','" + FuelLine_id + "','Fuel Sales','" + posId + "','" + EventEndDate + "','" + DiscountAmount + "','" + PromotionID + "','" + PromotionAmount + "')";
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
                                                DiscountAmount = null; LoyaltyDiscountAmount = null; LoyaltyEntryMethod = null; LoyaltyProgramName = null; CustomerLoyaltyId = null;
                                                if (ds.Tables["Discount"].Columns.Contains("ItemLine_Id"))
                                                {
                                                    for (int z = 0; z < ds.Tables["Discount"].Rows.Count; z++)
                                                    {
                                                        if (ds.Tables["ItemLine"].Rows[i]["ItemLine_Id"].ToString() == ds.Tables["Discount"].Rows[z]["ItemLine_Id"].ToString())
                                                        {
                                                            DiscountAmount = ds.Tables["Discount"].Rows[i]["DiscountAmount"].ToString();
                                                            // LoyaltyInfo
                                                            if (ds.Tables["Discount"].Rows[i]["DiscountReason"].ToString() == "loyaltyDiscount")
                                                            {
                                                                if (ds.Tables.Contains("LoyaltyInfo"))
                                                                {
                                                                    if (ds.Tables["LoyaltyInfo"].Columns.Contains("LoyaltyID"))
                                                                    {
                                                                        LoyaltyDiscountAmount = ds.Tables["Discount"].Rows[i]["DiscountAmount"].ToString();
                                                                        LoyaltyEntryMethod = ds.Tables["LoyaltyInfo"].Rows[0]["LoyaltyEntryMethod"].ToString();
                                                                        LoyaltyProgramName = ds.Tables["LoyaltyInfo"].Rows[0]["LoyaltyProgramName"].ToString();
                                                                        CustomerLoyaltyId = ds.Tables["LoyaltyInfo"].Rows[0]["LoyaltyID"].ToString();
                                                                    }
                                                                }
                                                            }
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
                                                string Query = "insert into ItemPJR  (TransactionId,Status,Format,POSCode,Descripation,Method,ActualSalesPrice,MerchandiseCode,SellingUnits,RegularSellPrice,SalesQuantity,SalesAmount,TaxLevelld,StoreId,ItemLine_Id,VoucherType,POSId,EventEndDate,PromotionID,PromotionAmount,DiscountAmount,CustomerLoyaltyId,LoyaltyProgramName,LoyaltyEntryMethod,LoyaltyDiscountAmount)  " +
                                                    "VALUES('" + TransactionID + "','normal','" + format + "','" + POSCode + "','" + Description + "','" + method + "','" + ActualSalesPrice + "','" + MerchandiseCode + "','" + SellingUnits + "','" + RegularSellPrice + "','" + SalesQuantity + "','" + SalesAmount + "','" + TaxLevelID + "','" + storeId + "','" + ItemLine_Id + "','Items Sales','" + posId + "','" + EventEndDate + "','" + PromotionID + "','" + PromotionAmount + "','" + DiscountAmount + "','" + CustomerLoyaltyId + "','" + LoyaltyProgramName + "','" + LoyaltyEntryMethod + "','" + LoyaltyDiscountAmount + "')";
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
                                                string Query = "insert into MerchandisePJR (TransactionId,Status,Descripation,MerchandiseCode,ActualSalesPrice,SalesQuantity,RegularSellPrice,SalesAmount,TaxLevelld,StoreId,VoucherType,POSId,EventEndDate,DiscountAmount,PromotionID,PromotionAmount)  " +
                                                    " VALUES('" + TransactionID + "','normal','" + M_Description + "','" + M_MerchandiseCode + "','" + M_ActualSalesPrice + "','" + M_SalesQuantity + "','" + M_RegularSellPrice + "','" + M_SalesAmount + "','" + TaxLevelID + "','" + storeId + "','Merchandise Sales','" + posId + "','" + EventEndDate + "','" + DiscountAmount + "','" + PromotionID + "','" + PromotionAmount + "')";
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
                                                string Query = "insert into TenderPJR (TransactionId,Status,TenderCode,TenderSubCode,TenderAmount,Value,StoreId,TenderInfo_Id,VoucherType,POSId,EventEndDate,AccountName)  " +
                                                    "VALUES('" + TransactionID + "','normal','" + TenderCode + "','" + TenderSubCode + "','" + TenderAmount + "','" + value + "','" + storeId + "','" + TenderInfo_Id + "','Tender Sales','" + posId + "','" + EventEndDate + "','" + AccountName + "')";
                                                conn.Open();
                                                SqlCommand cmd = new SqlCommand();
                                                cmd = new SqlCommand(Query, conn);
                                                cmd.ExecuteNonQuery();
                                                conn.Close();
                                            }
                                        }
                                    }
                                }
                                //string copypath = Path.GetDirectoryName("CopyFiles");
                                //GrantAccess(copypath);
                                string copyPath = basePath + "\\CopyFiles\\";
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
                                    if (folderName != null)
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
        public void Altria()
        {
            string errorFileName = "";
            try
            {
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
                    DataTable dt = new DataTable();
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
                            adp.Fill(dt);
                            con.Close();
                        }
                    }
                    var rowCount = dt.Rows[0].ItemArray[0];

                    if (int.Parse(rowCount.ToString()) != 0)
                    {
                        DataTable dtScandataSetting = new DataTable();
                        using (SqlConnection conn = new SqlConnection(conString))
                        {
                            using (SqlCommand cmd = new SqlCommand("select * from ScanDataSetting where StoreId=@StoreId and name=@type", conn))
                            {
                                //conn.Open();
                                cmd.Parameters.Add("@StoreId", SqlDbType.VarChar).Value = storeId;
                                cmd.Parameters.Add("@type", SqlDbType.VarChar).Value = "Altria";
                                SqlDataAdapter adp = new SqlDataAdapter();
                                adp.SelectCommand = cmd;
                                adp.Fill(dtScandataSetting);
                                //conn.Close();
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
                            string end_date = (nextWednesday.ToString("yyyy/MM/dd").Replace("-", "")).Replace("/", "");
                            string fileName = (dtFile.Rows[0].ItemArray[6].ToString() + end_date).Replace(" ", "") + ".txt";
                            string copyPath = basePath + "\\ScanData\\";
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
                                writer.WriteLine(string.Join("|", row.ItemArray.Select(x => x.ToString())));
                            }
                            string ftppath = dtScandataSetting.Rows[0]["Server"].ToString();

                            writer.Close();
                            string from = copyPath;
                            string user = dtScandataSetting.Rows[0]["UserName"].ToString();
                            string pass = dtScandataSetting.Rows[0]["Password"].ToString();

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
        private void Form1_Load(object sender, EventArgs e)
        {
        }
        public void RJR()
        {
            string errorFileName = "";
            try
            {
                DateTime lastSunday = DateTime.Now.AddDays(-1);
                while (lastSunday.DayOfWeek != DayOfWeek.Sunday)
                    lastSunday = lastSunday.AddDays(-1);
                DateTime lastMonday = DateTime.Now.AddDays(-8);
                while (lastMonday.DayOfWeek != DayOfWeek.Monday)
                    lastMonday = lastMonday.AddDays(-1);

                var startdate = lastMonday.ToString("MM/dd/yyyy").Replace("-", "/");
                var enddate = lastSunday.ToString("MM/dd/yyyy").Replace("-", "/");

                if (startdate != null && enddate != null && storeId != null && posId != null)
                {
                    DataTable ds = new DataTable();
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
                    var rowCount = ds.Rows[0].ItemArray[0];

                    if (int.Parse(rowCount.ToString()) != 0)
                    {

                        SqlConnection conn = new SqlConnection(conString);
                        string RJR = "RJR";
                        DataTable dtScandataSetting = new DataTable();
                        string query1 = "select * from ScanDataSetting where name='" + RJR + "' and StoreId=" + storeId;
                        SqlCommand cmd = new SqlCommand(query1, conn);
                        //conn.Open();
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        da.Fill(dtScandataSetting);
                        //conn.Close();
                        da.Dispose();

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
                                cmdFile.Parameters.Add("@type", SqlDbType.VarChar).Value = "RJR";
                                SqlDataAdapter adp = new SqlDataAdapter();
                                adp.SelectCommand = cmdFile;
                                adp.Fill(dtFile);
                                conFile.Close();
                            }
                        }
                        if (dtFile.Rows.Count != 0)
                        {
                            string end_date = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time")).ToString("yyyy/MM/dd").Replace("-", "").Replace("/", "");
                            string fileName = (dtFile.Rows[0].ItemArray[0].ToString() + "_" + end_date).Replace(" ", "") + ".json";
                            dtFile.Columns["OutletName"].ColumnName = "Outlet Name";
                            dtFile.Columns["OutletNumber"].ColumnName = "Outlet Number";
                            dtFile.Columns["OutletAddress1"].ColumnName = "Outlet Address 1";
                            dtFile.Columns["OutletAddress2"].ColumnName = "Outlet Address 2";
                            dtFile.Columns["OutletCity"].ColumnName = "Outlet City";
                            dtFile.Columns["OutletState"].ColumnName = "Outlet State";
                            dtFile.Columns["OutletZipCode"].ColumnName = "Outlet Zip Code";
                            dtFile.Columns["TransactionDateTime"].ColumnName = "Transaction Date/Time";
                            dtFile.Columns["MarketbasketTransactionId"].ColumnName = "Market Basket Transaction ID";
                            dtFile.Columns["ScanTransactionId"].ColumnName = "Scan Transaction ID";
                            dtFile.Columns["RegisterId"].ColumnName = "Register ID";
                            dtFile.Columns["Quantity"].ColumnName = "Quantity";
                            dtFile.Columns["Price"].ColumnName = "Price";
                            dtFile.Columns["UPCCode"].ColumnName = "UPC Code";
                            dtFile.Columns["UPCDescription"].ColumnName = "UPC Description";
                            dtFile.Columns["UnitOfMeasure"].ColumnName = "Unit of Measure";
                            dtFile.Columns["PromotionFlag"].ColumnName = "Promotion Flag";
                            dtFile.Columns["OutletMultipackFlag"].ColumnName = "Outlet Multi-Pack Flag";
                            dtFile.Columns["OutletMultipackQuantity"].ColumnName = "Outlet Multi-Pack Quantity";
                            dtFile.Columns["OutletMultipackDiscountAmount"].ColumnName = "Outlet Multi-Pack Discount Amount";
                            dtFile.Columns["AccountPromotionName"].ColumnName = "Account Promotion Name";
                            dtFile.Columns["AccountDiscountAmount"].ColumnName = "Account Discount Amount";
                            dtFile.Columns["ManufacturerDiscountAmount"].ColumnName = "Manufacturer Discount Amount";
                            dtFile.Columns["CouponPID"].ColumnName = "Coupon PID";
                            dtFile.Columns["CouponAmount"].ColumnName = "Coupon Amount";
                            dtFile.Columns["ManufacturerMultipackFlag"].ColumnName = "Manufacturer Multi-pack Flag";
                            dtFile.Columns["ManufacturerMultipackQuantity"].ColumnName = "Manufacturer Multi-pack Quantity";
                            dtFile.Columns["MfgMultipackDiscountAmount"].ColumnName = "Manufacturer Multi-pack Discount Amount";
                            dtFile.Columns["ManufacturerPromotionDescription"].ColumnName = "Manufacturer Promotion Description";
                            dtFile.Columns["ManufacturerBuydownDescription"].ColumnName = "Manufacturer Buy-down Description";
                            dtFile.Columns["ManufacturerBuydownAmount"].ColumnName = "Manufacturer Buy-down Amount";
                            dtFile.Columns["ManufacturerMultipackDescription"].ColumnName = "Manufacturer Multi-pack Description";
                            dtFile.Columns["AccountLoyaltyIDNumber"].ColumnName = "Account Loyalty ID Number";
                            dtFile.Columns["CouponDescription"].ColumnName = "Coupon Description";
                            var jsonStr = JsonConvert.SerializeObject(dtFile);

                            string path = basePath + "ScanData\\";
                            if (!Directory.Exists(path))
                            {
                                Directory.CreateDirectory(path);
                            }

                            string ftppath = dtScandataSetting.Rows[0]["Server"].ToString();
                            System.IO.File.WriteAllText(path + fileName, jsonStr);

                            string from = path + fileName;
                            string user = dtScandataSetting.Rows[0]["UserName"].ToString();
                            string pass = dtScandataSetting.Rows[0]["Password"].ToString();
                            SftpClient sftpClient = new SftpClient(ftppath, user, pass);
                            sftpClient.Connect();
                            FileStream fs = new FileStream(from, FileMode.Open);
                            sftpClient.UploadFile(fs, "/blgpalincxfer_live/incoming/" + Path.GetFileName(from));
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
        public void VerifoneRead()
        {
            string errorFileName = "";
            SqlConnection conn = new SqlConnection(conString);
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

                            if (str == "PJR")
                            {
                                var tranList = new List<Transaction>();
                                if (ds.Tables.Contains("trans"))
                                {
                                    DataTable dt = new DataTable();
                                    for (int i = 0; i < ds.Tables["trans"].Rows.Count; i++)
                                    {
                                        bool exists = false;
                                        Transaction transaction = new Transaction();
                                        if (ds.Tables["trans"].Rows[i]["type"].ToString() == "network sale" || ds.Tables["trans"].Rows[i]["type"].ToString() == "sale" || ds.Tables["trans"].Rows[i]["type"].ToString() == "refund sale")
                                        {
                                            if (ds.Tables.Contains("trHeader"))
                                            {
                                                var trHeader = ds.Tables["trHeader"].Select($"trans_Id = {ds.Tables["trans"].Rows[i]["trans_Id"].ToString()}");
                                                if (trHeader.Length != 0)
                                                {
                                                    if (ds.Tables["trHeader"].Columns.Contains("trUniqueSN"))
                                                    {
                                                        folderName = trHeader[0]["date"].ToString().Substring(0, 10).Replace("-", "");
                                                        transaction.TransactionId = trHeader[0]["trUniqueSN"].ToString();
                                                        transaction.EventEndDate = trHeader[0]["date"].ToString().Substring(0, 10);
                                                        transaction.EventEndTime = trHeader[0]["date"].ToString().Substring(11).Substring(0, 8);

                                                        if (dt.Rows.Count == 0 || dt.Rows[0]["EventEndDate"].ToString() != transaction.EventEndDate)
                                                        {
                                                            dt.Reset();
                                                            string Query = " Select trans.TransactionId,EventEndDate,case when Sum(convert(decimal(10,2),TransactionTotalGrandAmount))!=Sum(convert(decimal(10,2),TenderAmount)) and Sum(convert(decimal(10,2),TransactionTotalGrossAmount))!=Sum(convert(decimal(10,2),SalesAmount)) then 0 else 1 end as valid  from(select Transactionid,EventEndDate,TransactionTotalGrandAmount,TransactionTotalGrossAmount from TransactionPJR where Storeid='" + storeId + "' and EventEndDate='" + transaction.EventEndDate + "')as trans join " +
                                                                            " (select TransactionId,SalesAmount from Itempjr where Storeid='" + storeId + "' and EventEndDate='" + transaction.EventEndDate + "' union all select TransactionId,SalesAmount from MerchandisePJR where Storeid='" + storeId + "' and EventEndDate='" + transaction.EventEndDate + "' union all select TransactionId,SalesAmount from fuelPJR where Storeid='" + storeId + "' and EventEndDate='" + transaction.EventEndDate + "' union all " +
                                                                            " select TransactionId,SalesAmount from FuelPrePay where Storeid='" + storeId + "' and Vouchertype='" + transaction.EventEndDate + "' )as x on trans.TransactionId=x.TransactionId join (select TransactionId,Sum(convert(decimal(10,2),TenderAmount))as TenderAmount from Tenderpjr where Storeid='" + storeId + "' and EventEndDate='" + transaction.EventEndDate + "'  group by TransactionId)as y  on trans.TransactionId=y.TransactionId " +
                                                                            " Group by trans.TransactionId,EventEndDate ";
                                                            SqlDataAdapter sda = new SqlDataAdapter(Query, conn);
                                                            sda.SelectCommand.CommandTimeout = 100000;
                                                            sda.Fill(dt);
                                                        }
                                                        if (dt.Rows.Count != 0)
                                                        {
                                                            if (transaction.TransactionId != "")
                                                            {
                                                                DataRow[] row = dt.Select("TransactionId = " + transaction.TransactionId);
                                                                if (row.Length != 0)
                                                                    if (dt.Select("TransactionId = " + transaction.TransactionId)[0]["valid"].ToString() == "1") { exists = true; }
                                                            }
                                                            else
                                                            {
                                                                exists = true;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if (!exists)
                                            {


                                                if (ds.Tables.Contains("trPaylines"))
                                                {
                                                    var trPaylines = ds.Tables["trPaylines"].Select($"trans_Id = {ds.Tables["trans"].Rows[i]["trans_Id"].ToString()}");
                                                    if (ds.Tables.Contains("trPayline"))
                                                    {
                                                        var trPayline = ds.Tables["trPayline"].Select($"trPayLines_Id = {trPaylines[0]["trPayLines_Id"].ToString()}");
                                                        for (int t = 0; t < trPayline.Length; t++)
                                                        {
                                                            Tender tender = new Tender();
                                                            var trpPaycode = ds.Tables["trpPaycode"].Select($"trPayLine_Id = {trPayline[t]["trPayLine_Id"].ToString()}");
                                                            tender.TenderCode = trpPaycode[0]["trpPaycode_Text"].ToString();
                                                            tender.TenderSubCode = trpPaycode[0]["nacstendersubcode"].ToString();
                                                            tender.TenderAmount = trPayline[t]["trpAmt"].ToString();
                                                            if (ds.Tables["trans"].Rows[i]["type"].ToString() == "refund sale") { tender.TenderAmount = (Convert.ToDecimal(tender.TenderAmount) * -1).ToString(); }

                                                            string Querytn = " Declare @count int=(select Count(TransactionID) from TenderPJR where Storeid='" + storeId + "' and POSId='" + posId + "' and EventEndDate='" + transaction.EventEndDate + "' and TransactionId='" + transaction.TransactionId + "' and TenderInfo_Id='" + t + "')  if(@count=0) begin " +
                                                                " insert into TenderPJR(TransactionId,EventEndDate,TenderCode,TenderSubCode,TenderAmount,TenderInfo_Id,StoreId,VoucherType,POSId) " +
                                                                "VALUES('" + transaction.TransactionId + "','" + transaction.EventEndDate + "','" + tender.TenderCode + "','" + tender.TenderSubCode + "','" + tender.TenderAmount + "','" + t + "','" + storeId + "','Tender Sales','" + posId + "') " +
                                                                " end ";
                                                            conn.Open();
                                                            SqlCommand cmdtn = new SqlCommand();
                                                            cmdtn = new SqlCommand(Querytn, conn);
                                                            cmdtn.ExecuteNonQuery();
                                                            conn.Close();
                                                        }
                                                    }
                                                }
                                                if (ds.Tables.Contains("trLines"))
                                                {
                                                    var trLines = ds.Tables["trLines"].Select($"trans_Id = {ds.Tables["trans"].Rows[i]["trans_Id"].ToString()}");
                                                    if (ds.Tables.Contains("trLine"))
                                                    {
                                                        var trLine = ds.Tables["trLine"].Select($"trLines_Id = {trLines[0]["trLines_Id"].ToString()}");
                                                        for (int s = 0; s < trLine.Length; s++)
                                                        {
                                                            if (trLine[s]["type"].ToString() == "plu")
                                                            {
                                                                Item itemPJR = new Item();
                                                                itemPJR.POSCode = trLine[s]["trlUPC"].ToString();
                                                                itemPJR.Descripation = trLine[s]["trlDesc"].ToString();
                                                                itemPJR.Format = trLine[s]["type"].ToString();
                                                                itemPJR.SalesQuantity = trLine[s]["trlQty"].ToString();
                                                                itemPJR.SellingUnits = trLine[s]["trlSellUnit"].ToString();
                                                                itemPJR.SalesAmount = trLine[s]["trlLineTot"].ToString();
                                                                itemPJR.RegularSellPrice = trLine[s]["trlUnitPrice"].ToString();
                                                                itemPJR.ActualSalesPrice = trLine[s]["trlUnitPrice"].ToString();
                                                                itemPJR.MerchandiseCode = ds.Tables["trlDept"].Select($"trLine_Id={trLine[s]["trLine_Id"].ToString()}")[0]["number"].ToString();
                                                                if (ds.Tables.Contains("trlMixMatches"))
                                                                {
                                                                    var mixmatchId = ds.Tables["trlMixMatches"].Select($"trLine_Id={trLine[s]["trLine_Id"].ToString()}");
                                                                    if (mixmatchId.Length != 0)
                                                                    {
                                                                        if (ds.Tables.Contains("trlMatchLine"))
                                                                        {

                                                                            for (int m = 0; m < ds.Tables["trlMatchLine"].Rows.Count; m++)
                                                                            {
                                                                                if (ds.Tables["trlMatchLine"].Rows[m]["trlMixMatches_Id"].ToString() == mixmatchId[0]["trlMixMatches_Id"].ToString())
                                                                                {
                                                                                    if (ds.Tables.Contains("trlPromotionID"))
                                                                                    {
                                                                                        if (itemPJR.PromotionID == "" || itemPJR.PromotionID is null)
                                                                                            itemPJR.PromotionID = ds.Tables["trlPromotionID"].Select($"trlMatchLine_Id={ds.Tables["trlMatchLine"].Rows[m]["trlMatchLine_Id"].ToString()}")[0]["trlPromotionID_Text"].ToString();
                                                                                        else
                                                                                            itemPJR.PromotionID = itemPJR.PromotionID + "," + ds.Tables["trlPromotionID"].Select($"trlMatchLine_Id={ds.Tables["trlMatchLine"].Rows[m]["trlMatchLine_Id"].ToString()}")[0]["trlPromotionID_Text"].ToString();
                                                                                    }
                                                                                    if (ds.Tables["trlMatchLine"].Columns.Contains("trlPromoAmount"))
                                                                                    {
                                                                                        if (itemPJR.PromotionAmount == "" || itemPJR.PromotionAmount is null)
                                                                                            itemPJR.PromotionAmount = ds.Tables["trlMatchLine"].Rows[m]["trlPromoAmount"].ToString();
                                                                                        else
                                                                                            itemPJR.PromotionAmount = (Convert.ToDecimal(itemPJR.PromotionAmount) + Convert.ToDecimal(ds.Tables["trlMatchLine"].Rows[m]["trlPromoAmount"].ToString())).ToString();
                                                                                    }
                                                                                    itemPJR.SalesAmount = ds.Tables["trlMatchLine"].Rows[m]["trlMatchPrice"].ToString();
                                                                                    itemPJR.ActualSalesPrice = ds.Tables["trlMatchLine"].Rows[m]["trlMatchPrice"].ToString();
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                                itemPJR.LoyaltyDiscountAmount = null;
                                                                itemPJR.LoyaltyEntryMethod = null;
                                                                itemPJR.LoyaltyProgramName = null;
                                                                itemPJR.CustomerLoyaltyId = null;

                                                                if (transaction.TransactionId != "")
                                                                {
                                                                    string Queryi = " Declare @count int=(select Count(TransactionID) from ItemPJR where Storeid='" + storeId + "' and POSId='" + posId + "' and EventEndDate='" + transaction.EventEndDate + "' and TransactionId='" + transaction.TransactionId + "' and ItemLine_Id='" + s + "')  if(@count=0) begin " +
                                                                        " insert into ItemPJR(TransactionId,EventEndDate,POSCode,Format,Descripation,ActualSalesPrice,MerchandiseCode,SellingUnits,RegularSellPrice,SalesQuantity,SalesAmount,ItemLine_Id,StoreId,VoucherType,POSId,PromotionID,PromotionAmount,CustomerLoyaltyId,LoyaltyProgramName,LoyaltyEntryMethod,LoyaltyDiscountAmount) " +
                                                                    "VALUES('" + transaction.TransactionId + "','" + transaction.EventEndDate + "','" + itemPJR.POSCode + "','" + itemPJR.Format + "','" + itemPJR.Descripation + "','" + itemPJR.ActualSalesPrice + "','" + itemPJR.MerchandiseCode + "','" + itemPJR.SellingUnits + "','" + itemPJR.RegularSellPrice + "','" + itemPJR.SalesQuantity + "',Convert(decimal(10,2),'" + itemPJR.SalesAmount + "'),'" + s + "','" + storeId + "','Items Sales','" + posId + "','" + itemPJR.PromotionID + "','" + itemPJR.PromotionAmount + "','" + itemPJR.CustomerLoyaltyId + "','" + itemPJR.LoyaltyProgramName + "','" + itemPJR.LoyaltyEntryMethod + "','" + itemPJR.LoyaltyDiscountAmount + "') " +
                                                                        " end ";
                                                                    conn.Open();
                                                                    SqlCommand cmdi = new SqlCommand();
                                                                    cmdi = new SqlCommand(Queryi, conn);
                                                                    cmdi.ExecuteNonQuery();
                                                                    conn.Close();
                                                                }

                                                            }
                                                            else if (trLine[s]["type"].ToString() == "void plu")
                                                            {
                                                                Item itemPJR = new Item();
                                                                itemPJR.POSCode = trLine[s]["trlUPC"].ToString();
                                                                itemPJR.Descripation = trLine[s]["trlDesc"].ToString();
                                                                itemPJR.Format = trLine[s]["type"].ToString();
                                                                itemPJR.SalesQuantity = (Convert.ToInt32(trLine[s]["trlQty"]) * -1).ToString();
                                                                itemPJR.SellingUnits = (Convert.ToDecimal(trLine[s]["trlSellUnit"]) * -1).ToString();
                                                                itemPJR.SalesAmount = trLine[s]["trlLineTot"].ToString();
                                                                itemPJR.RegularSellPrice = trLine[s]["trlUnitPrice"].ToString();
                                                                itemPJR.ActualSalesPrice = trLine[s]["trlUnitPrice"].ToString();
                                                                itemPJR.MerchandiseCode = ds.Tables["trlDept"].Select($"trLine_Id={trLine[s]["trLine_Id"].ToString()}")[0]["number"].ToString();
                                                                string Queryiv = " Declare @count int=(select Count(TransactionID) from ItemPJR where Storeid='" + storeId + "' and POSId='" + posId + "' and EventEndDate='" + transaction.EventEndDate + "' and TransactionId='" + transaction.TransactionId + "' and ItemLine_Id='" + s + "')  if(@count=0) begin " +
                                                                    " insert into ItemPJR(TransactionId,EventEndDate,POSCode,Format,Descripation,ActualSalesPrice,MerchandiseCode,SellingUnits,RegularSellPrice,SalesQuantity,SalesAmount,ItemLine_Id,StoreId,VoucherType,POSId) " +
                                                                "VALUES('" + transaction.TransactionId + "','" + transaction.EventEndDate + "','" + itemPJR.POSCode + "','" + itemPJR.Format + "','" + itemPJR.Descripation + "','" + itemPJR.ActualSalesPrice + "','" + itemPJR.MerchandiseCode + "','" + itemPJR.SellingUnits + "','" + itemPJR.RegularSellPrice + "','" + itemPJR.SalesQuantity + "',Convert(decimal(10,2),'" + itemPJR.SalesAmount + "'),'" + s + "','" + storeId + "','Items Sales','" + posId + "') " +
                                                                    " end ";
                                                                conn.Open();
                                                                SqlCommand cmdiv = new SqlCommand();
                                                                cmdiv = new SqlCommand(Queryiv, conn);
                                                                cmdiv.ExecuteNonQuery();
                                                                conn.Close();

                                                            }
                                                            else if (trLine[s]["type"].ToString() == "dept")
                                                            {
                                                                if (transaction.TransactionId != "")
                                                                {
                                                                    string Queryd = "Declare @count int= (select Count(TransactionID) from MerchandisePJR where Storeid = '" + storeId + "' and POSId = '" + posId + "' and EventEndDate = '" + transaction.EventEndDate + "' and TransactionId = '" + transaction.TransactionId + "' and TaxLevelld = '" + s + "')  if (@count = 0) begin " +
                                                                    "Insert into  MerchandisePJR(TransactionId,Descripation,MerchandiseCode,ActualSalesPrice,SalesQuantity,SalesAmount,StoreId,VoucherType,POSId,EventEndDate,TaxLevelld) Values " +
                                                                    " ('" + transaction.TransactionId + "','" + trLine[s]["trlDesc"].ToString() + "','" + ds.Tables["trlDept"].Select($"trLine_Id={trLine[s]["trLine_Id"].ToString()}")[0]["number"].ToString() + "','" + trLine[s]["trlUnitPrice"].ToString() + "','" + trLine[s]["trlQty"].ToString() + "',Convert(decimal(10,2),'" + trLine[s]["trlLineTot"].ToString() + "'),'" + storeId + "','Merchandise Sales','" + posId + "','" + transaction.EventEndDate + "','" + s + "') end";
                                                                    conn.Open();
                                                                    SqlCommand cmdd = new SqlCommand();
                                                                    cmdd = new SqlCommand(Queryd, conn);
                                                                    cmdd.ExecuteNonQuery();
                                                                    conn.Close();
                                                                }
                                                            }
                                                            else if (trLine[s]["type"].ToString() == "void dept")
                                                            {
                                                                if (transaction.TransactionId != "")
                                                                {
                                                                    string Querydv = "Declare @count int= (select Count(TransactionID) from MerchandisePJR where Storeid = '" + storeId + "' and POSId = '" + posId + "' and EventEndDate = '" + transaction.EventEndDate + "' and TransactionId = '" + transaction.TransactionId + "' and TaxLevelld = '" + s + "')  if (@count = 0) begin " +
                                                                    "Insert into  MerchandisePJR(TransactionId,Descripation,MerchandiseCode,ActualSalesPrice,SalesQuantity,SalesAmount,StoreId,VoucherType,POSId,EventEndDate,TaxLevelld) Values " +
                                                                    " ('" + transaction.TransactionId + "','" + trLine[s]["trlDesc"].ToString() + "','" + ds.Tables["trlDept"].Select($"trLine_Id={trLine[s]["trLine_Id"].ToString()}")[0]["number"].ToString() + "','" + trLine[s]["trlUnitPrice"].ToString() + "','" + (Convert.ToInt32(trLine[s]["trlQty"]) * -1).ToString() + "',Convert(decimal(10,2),'" + trLine[s]["trlLineTot"].ToString() + "'),'" + storeId + "','Merchandise Sales','" + posId + "','" + transaction.EventEndDate + "','" + s + "') end";
                                                                    conn.Open();
                                                                    SqlCommand cmddv = new SqlCommand();
                                                                    cmddv = new SqlCommand(Querydv, conn);
                                                                    cmddv.ExecuteNonQuery();
                                                                    conn.Close();
                                                                }
                                                            }
                                                            else if (trLine[s]["type"].ToString() == "postFuel")
                                                            {
                                                                if (transaction.TransactionId != "")
                                                                {
                                                                    var trlFuel = ds.Tables["trlFuel"].Select($"trLine_Id = {trLine[s]["trLine_Id"].ToString()}");
                                                                    var fuelProd = ds.Tables["fuelProd"].Select($"trlFuel_Id = {trlFuel[0]["trlFuel_Id"].ToString()}");

                                                                    string Queryf = "Declare @count int= (select Count(TransactionID) from fuelpjr where Storeid = '" + storeId + "' and POSId = '" + posId + "' and EventEndDate = '" + transaction.EventEndDate + "' and TransactionId = '" + transaction.TransactionId + "' and FuelLine_Id = '" + s + "')  if (@count = 0) begin " +
                                                                    "insert into fuelpjr(TransactionId,FuelGradeId,FuelPossitionId,Description,ActualSalesPrice,MerchandiseCode,RegularSellPrice,SalesQuantity,SalesAmount,StoreId,FuelLine_id,VoucherType,POSId,EventEndDate) Values" +
                                                                    " ('" + transaction.TransactionId + "','" + fuelProd[0]["NAXMLFuelGradeID"].ToString() + "','" + trlFuel[0]["fuelPosition"].ToString() + "','" + fuelProd[0]["fuelProd_Text"].ToString() + "','" + trLine[s]["trlUnitPrice"].ToString() + "','" + ds.Tables["trlDept"].Select($"trLine_Id={trLine[s]["trLine_Id"].ToString()}")[0]["number"].ToString() + "','" + trlFuel[0]["basePrice"].ToString() + "','" + trlFuel[0]["fuelVolume"].ToString() + "',Convert(decimal(10,2),'" + trLine[s]["trlLineTot"].ToString() + "'),'" + storeId + "','" + s + "','Fuel Sales','" + posId + "','" + transaction.EventEndDate + "') end";
                                                                    conn.Open();
                                                                    SqlCommand cmdf = new SqlCommand();
                                                                    cmdf = new SqlCommand(Queryf, conn);
                                                                    cmdf.ExecuteNonQuery();
                                                                    conn.Close();
                                                                }
                                                            }
                                                            else if (trLine[s]["type"].ToString() == "preFuel")
                                                            {
                                                                if (transaction.TransactionId != "")
                                                                {
                                                                    string Querypf = "Declare @count int= (select Count(TransactionID) from FuelPrePay where Storeid = '" + storeId + "' and Vouchertype = '" + transaction.EventEndDate + "' and TransactionId = '" + transaction.TransactionId + "' and FuelPositionId = '" + s + "')  if (@count = 0) begin " +
                                                                    "insert into FuelPrePay(TransactionId,FuelPositionId,SalesAmount,StoreId,VoucherType) Values " +
                                                                    " ('" + transaction.TransactionId + "','" + s + "',Convert(decimal(10,2),'" + trLine[s]["trlLineTot"].ToString() + "'),'" + storeId + "','" + transaction.EventEndDate + "') end";
                                                                    conn.Open();
                                                                    SqlCommand cmdpf = new SqlCommand();
                                                                    cmdpf = new SqlCommand(Querypf, conn);
                                                                    cmdpf.ExecuteNonQuery();
                                                                    conn.Close();
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                if (ds.Tables.Contains("trValue"))
                                                {
                                                    var trValue = ds.Tables["trValue"].Select($"trans_Id = {ds.Tables["trans"].Rows[i]["trans_Id"].ToString()}");
                                                    if (trValue.Length != 0)
                                                    {
                                                        transaction.TransactionTotalGrossAmount = trValue[0]["trTotNoTax"].ToString();
                                                        transaction.TransactionTotalGrandAmount = trValue[0]["trTotWTax"].ToString();
                                                        transaction.TransactionTotalTaxNetAmount = trValue[0]["trTotTax"].ToString();
                                                    }
                                                }

                                                string Queryt = "Declare @count int=(select Count(TransactionID) from TransactionPJR where Storeid='" + storeId + "' and POSId='" + posId + "' and EventEndDate='" + transaction.EventEndDate + "' and TransactionId='" + transaction.TransactionId + "')  if(@count=0) begin   " +
                                                    "Insert into TransactionPJR(TransactionId,EventEndDate,EventEndTime,TransactionTotalGrossAmount,TransactionTotalTaxNetAmount,TransactionTotalGrandAmount,StoreId,VoucherType,POSId) VALUES  " +
                                                    "('" + transaction.TransactionId + "','" + transaction.EventEndDate + "','" + transaction.EventEndTime + "','" + transaction.TransactionTotalGrossAmount + "','" + transaction.TransactionTotalTaxNetAmount + "','" + transaction.TransactionTotalGrandAmount + "','" + storeId + "','Sales Transaction','" + posId + "')  end ";

                                                conn.Open();
                                                SqlCommand cmdt = new SqlCommand();
                                                cmdt = new SqlCommand(Queryt, conn);
                                                cmdt.ExecuteNonQuery();
                                                conn.Close();
                                            }
                                        }
                                    }
                                }
                            }
                            else if (str == "MSM")
                            {
                                string startDate = Convert.ToDateTime(ds.Tables["period"].Select($"isFirst =" + true)[0]["periodBeginDate"].ToString()).ToString("yyyy-MM-dd");
                                string endDate = Convert.ToDateTime(ds.Tables["period"].Select($"isLast =" + true)[0]["periodEndDate"].ToString()).ToString("yyyy-MM-dd");
                                string startTime = Convert.ToDateTime(ds.Tables["period"].Select($"isFirst =" + true)[0]["periodBeginDate"].ToString()).ToString("HH:mm:ss");
                                string endTime = Convert.ToDateTime(ds.Tables["period"].Select($"isLast =" + true)[0]["periodEndDate"].ToString()).ToString("HH:mm:ss");
                                string taxableSale = "0";
                                string nonTaxableSales = "0";
                                string netTax = "0";
                                for (int i = 0; i < ds.Tables["taxInfo"].Rows.Count; i++)
                                {
                                    string dd = (ds.Tables["taxrateBase"].Select($"taxInfo_Id = {ds.Tables["taxInfo"].Rows[i]["taxInfo_Id"].ToString()}"))[0]["taxRate"].ToString();
                                    if (Convert.ToDecimal(dd) != Convert.ToDecimal("0"))
                                    {
                                        if (ds.Tables["taxInfo"].Rows[i]["totals_Id"].ToString() == "0")
                                        {
                                            taxableSale = (Convert.ToDecimal(taxableSale) + Convert.ToDecimal(ds.Tables["taxInfo"].Rows[i]["taxableSales"].ToString()) + Convert.ToDecimal(ds.Tables["taxInfo"].Rows[i]["taxableRefunds"].ToString())).ToString();
                                            nonTaxableSales = (Convert.ToDecimal(nonTaxableSales) + Convert.ToDecimal(ds.Tables["taxInfo"].Rows[i]["nonTaxableSales"].ToString()) + Convert.ToDecimal(ds.Tables["taxInfo"].Rows[i]["taxExemptSales"].ToString())
                                                + Convert.ToDecimal(ds.Tables["taxInfo"].Rows[i]["taxExemptRefunds"].ToString()) + Convert.ToDecimal(ds.Tables["taxInfo"].Rows[i]["taxForgivenSales"].ToString())
                                                + Convert.ToDecimal(ds.Tables["taxInfo"].Rows[i]["taxForgivenRefunds"].ToString())).ToString();
                                            netTax = (Convert.ToDecimal(netTax) + Convert.ToDecimal(ds.Tables["taxInfo"].Rows[i]["netTax"].ToString())).ToString();
                                        }
                                    }
                                }

                                string Querymsm = "If((Select Count(MSMCId) from MSMC Where StartDate= '" + startDate + "' and EndDate='" + endDate + "' and StartTime='" + startTime + "' and EndTime= '" + endTime + "' and StoreId='" + storeId + "'  and POSID='" + posId + "' and TaxableSales='" + taxableSale + "')=0) Begin  " +
                                    "Insert into MSMC (StartDate,EndDate,StartTime,EndTime,TaxableSales,NonTaxableSales,TaxSales,StoreId,POSId) Values  " +
                                    "('" + startDate + "','" + endDate + "','" + startTime + "','" + endTime + "','" + taxableSale + "','" + nonTaxableSales + "','" + netTax + "','" + storeId + "','" + posId + "') End ";
                                conn.Open();
                                SqlCommand cmdmsm = new SqlCommand();
                                cmdmsm = new SqlCommand(Querymsm, conn);
                                cmdmsm.ExecuteNonQuery();
                                conn.Close();
                            }
                            else if (str == "FGM")
                            {
                                string startDate = Convert.ToDateTime(ds.Tables["period"].Select($"isFirst =" + true)[0]["periodBeginDate"].ToString()).ToString("yyyy-MM-dd");
                                string endDate = Convert.ToDateTime(ds.Tables["period"].Select($"isLast =" + true)[0]["periodEndDate"].ToString()).ToString("yyyy-MM-dd");
                                string startTime = Convert.ToDateTime(ds.Tables["period"].Select($"isFirst =" + true)[0]["periodBeginDate"].ToString()).ToString("HH:mm:ss");
                                string endTime = Convert.ToDateTime(ds.Tables["period"].Select($"isLast =" + true)[0]["periodEndDate"].ToString()).ToString("HH:mm:ss");
                                for (int i = 0; i < ds.Tables["fuelProdBase"].Rows.Count; i++)
                                {
                                    string number = ds.Tables["fuelProdBase"].Rows[i]["number"].ToString();
                                    string name = ds.Tables["fuelProdBase"].Rows[i]["name"].ToString();
                                    string amount = ds.Tables["fuelInfo"].Select($"productInfo_Id =" + ds.Tables["fuelProdBase"].Rows[i]["productInfo_Id"].ToString())[0]["amount"].ToString();
                                    string volume = ds.Tables["fuelInfo"].Select($"productInfo_Id =" + ds.Tables["fuelProdBase"].Rows[i]["productInfo_Id"].ToString())[0]["volume"].ToString();

                                    string Querymsm = "IF((Select Count(FGMId) from FGM Where StartDate= '" + startDate + "' and EndDate='" + endDate + "' and StartTime='" + startTime + "' and EndTime= '" + endTime + "' and Number= '" + number + "' and StoreId='" + storeId + "'  and POSID='" + posId + "')=0) Begin  " +
                                        "Insert into FGM (StartDate,EndDate,StartTime,EndTime,Name,Number,volume,Amount,StoreId,POSId) Values  " +
                                        "('" + startDate + "','" + endDate + "','" + startTime + "','" + endTime + "','" + name + "','" + number + "','" + volume + "','" + amount + "','" + storeId + "','" + posId + "') End ";
                                    conn.Open();
                                    SqlCommand cmdmsm = new SqlCommand();
                                    cmdmsm = new SqlCommand(Querymsm, conn);
                                    cmdmsm.ExecuteNonQuery();
                                    conn.Close();
                                }
                            }
                            else if (str == "MCM")
                            {
                                string startDate = Convert.ToDateTime(ds.Tables["period"].Select($"isFirst =" + true)[0]["periodBeginDate"].ToString()).ToString("yyyy-MM-dd");
                                string endDate = Convert.ToDateTime(ds.Tables["period"].Select($"isLast =" + true)[0]["periodEndDate"].ToString()).ToString("yyyy-MM-dd");
                                string startTime = Convert.ToDateTime(ds.Tables["period"].Select($"isFirst =" + true)[0]["periodBeginDate"].ToString()).ToString("HH:mm:ss");
                                string endTime = Convert.ToDateTime(ds.Tables["period"].Select($"isLast =" + true)[0]["periodEndDate"].ToString()).ToString("HH:mm:ss");
                                for (int i = 0; i < ds.Tables["deptInfo"].Rows.Count; i++)
                                {
                                    if (ds.Tables["deptInfo"].Rows[i]["totals_Id"].ToString() == "0")
                                    {
                                        if (ds.Tables["deptBase"].Rows[i]["deptType"].ToString() == "norm")
                                        {
                                            string code = ds.Tables["deptBase"].Select($"deptInfo_Id =" + ds.Tables["deptInfo"].Rows[i]["deptInfo_Id"].ToString())[0]["sysid"].ToString();
                                            string descr = ds.Tables["deptBase"].Select($"deptInfo_Id =" + ds.Tables["deptInfo"].Rows[i]["deptInfo_Id"].ToString())[0]["name"].ToString();
                                            string saleAmt = ds.Tables["netSales"].Select($"deptInfo_Id =" + ds.Tables["deptInfo"].Rows[i]["deptInfo_Id"].ToString())[0]["amount"].ToString();
                                            string saleCount = ds.Tables["netSales"].Select($"deptInfo_Id =" + ds.Tables["deptInfo"].Rows[i]["deptInfo_Id"].ToString())[0]["count"].ToString();
                                            string refundsAmt = ds.Tables["refunds"].Select($"deptInfo_Id =" + ds.Tables["deptInfo"].Rows[i]["deptInfo_Id"].ToString())[0]["amount"].ToString();
                                            string refundsCount = ds.Tables["refunds"].Select($"deptInfo_Id =" + ds.Tables["deptInfo"].Rows[i]["deptInfo_Id"].ToString())[0]["count"].ToString();
                                            string discId = ds.Tables["discounts"].Select($"deptInfo_Id =" + ds.Tables["deptInfo"].Rows[i]["deptInfo_Id"].ToString())[0]["discounts_Id"].ToString();

                                            string discCount = ds.Tables["manualDiscounts"].Select($"discounts_Id =" + discId)[0]["count"].ToString();
                                            string discAmt = ds.Tables["manualDiscounts"].Select($"discounts_Id =" + discId)[0]["amount"].ToString();
                                            string promoCount = ds.Tables["promotions"].Select($"discounts_Id =" + discId)[0]["count"].ToString();
                                            string promoAmt = ds.Tables["promotions"].Select($"discounts_Id =" + discId)[0]["amount"].ToString();

                                            string Querymsm = "IF((Select Count(BeginDate) from MCM  Where BeginDate= '" + startDate + "' and EndDate='" + endDate + "' and BeginTime='" + startTime + "' and EndTime= '" + endTime + "' and MerchandiseCodeDescription= '" + descr + "' and StoreId='" + storeId + "'  and POSID='" + posId + "' and SalesAmount='" + saleAmt + "')=0) Begin  " +
                                            "Insert Into MCM (BeginDate,BeginTime,EndDate,EndTime,MerchandiseCode,MerchandiseCodeDescription,DiscountAmount,DiscountCount,PromotionAmount,PromotionCount,RefundAmount,RefundCount,SalesAmount,SalesQuantity,StoreId,POSId) Values  " +
                                            "( '" + startDate + "','" + startTime + "','" + endDate + "','" + endTime + "','" + code + "','" + descr + "','" + discAmt + "','" + discCount + "','" + promoAmt + "','" + promoCount + "','" + refundsAmt + "','" + refundsCount + "','" + saleAmt + "','" + saleCount + "','" + storeId + "','" + posId + "' ) End ";
                                            conn.Open();
                                            SqlCommand cmdmsm = new SqlCommand();
                                            cmdmsm = new SqlCommand(Querymsm, conn);
                                            cmdmsm.ExecuteNonQuery();
                                            conn.Close();

                                        }
                                    }


                                }
                            }
                            else if (str == "SUM")
                            {
                                string startDate = Convert.ToDateTime(ds.Tables["period"].Select($"isFirst =" + true)[0]["periodBeginDate"].ToString()).ToString("yyyy-MM-dd");
                                string endDate = Convert.ToDateTime(ds.Tables["period"].Select($"isLast =" + true)[0]["periodEndDate"].ToString()).ToString("yyyy-MM-dd");
                                string startTime = Convert.ToDateTime(ds.Tables["period"].Select($"isFirst =" + true)[0]["periodBeginDate"].ToString()).ToString("HH:mm:ss");
                                string endTime = Convert.ToDateTime(ds.Tables["period"].Select($"isLast =" + true)[0]["periodEndDate"].ToString()).ToString("HH:mm:ss");

                                for (int i = 0; i < ds.Tables["totals"].Rows.Count; i++)
                                {
                                    string summaryInfoId = ds.Tables["summaryInfo"].Select($"totals_Id =" + ds.Tables["totals"].Rows[i]["totals_Id"].ToString())[0]["summaryInfo_Id"].ToString();
                                    string mopTotalsId = ds.Tables["mopTotals"].Select($"summaryInfo_Id =" + summaryInfoId)[0]["mopTotals_Id"].ToString();
                                    string saleId = ds.Tables["sale"].Select($"mopTotals_Id =" + mopTotalsId)[0]["sale_Id"].ToString();
                                    if (ds.Tables["mopInfo"].Select($"sale_Id={saleId}").Count() != 0)
                                    {
                                        string cash = "";
                                        string credit = "";
                                        string debit = "";
                                        string inHouse = "";
                                        string foodStamp = "";
                                        string mobile = "";
                                        if (ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='CASH'").Count() != 0)
                                            cash = ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='CASH'")[0]["amount"].ToString();
                                        if (ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='CREDIT'").Count() != 0)
                                            credit = ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='CREDIT'")[0]["amount"].ToString();
                                        if (ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='DEBIT'").Count() != 0)
                                            debit = ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='DEBIT'")[0]["amount"].ToString();
                                        if (ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='IN-HOUSE'").Count() != 0)
                                            inHouse = ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='IN-HOUSE'")[0]["amount"].ToString();
                                        if (ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='FOODSTAMP'").Count() != 0)
                                            foodStamp = ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='FOODSTAMP'")[0]["amount"].ToString();
                                        if (ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='MOBILE'").Count() != 0)
                                            mobile = ds.Tables["mopInfo"].Select($"sale_Id={saleId}").CopyToDataTable().Select($"name='MOBILE'")[0]["amount"].ToString();

                                        string Querymsm = "If((Select Count(MSMCId) from MSMC Where StartDate= '" + startDate + "' and EndDate='" + endDate + "' and StartTime='" + startTime + "' and EndTime= '" + endTime + "' and Cash= '" + cash + "' and Credit= '" + credit + "' and Debit= '" + debit + "' and Foodstamp= '" + foodStamp + "' and Mobile= '" + mobile + "' and InHouse= '" + inHouse + "' and StoreId='" + storeId + "'  and POSID='" + posId + "')=0) Begin  " +
                                       "Insert into MSMC (StartDate,EndDate,StartTime,EndTime,Cash,Credit,Debit,Foodstamp,Mobile,InHouse,StoreId,POSId) Values  " +
                                        "('" + startDate + "','" + endDate + "','" + startTime + "','" + endTime + "','" + cash + "','" + credit + "','" + debit + "','" + foodStamp + "','" + mobile + "','" + inHouse + "','" + storeId + "','" + posId + "') End ";
                                        conn.Open();
                                        SqlCommand cmdmsm = new SqlCommand();
                                        cmdmsm = new SqlCommand(Querymsm, conn);
                                        cmdmsm.ExecuteNonQuery();
                                        conn.Close();
                                    }
                                }
                            }

                            string copyPath = basePath + "\\CopyFiles\\";
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
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SendErrorToText(ex, "VerifoneRead");
            }
        }
        public void copyFile()
        {
            SqlConnection conn = new SqlConnection(conString);
            try
            {
                var basePathPOS = basePath + "POS";
                if (!Directory.Exists(basePath))
                    Directory.CreateDirectory(basePath);

                string queryString = "Select distinct FilePath from (Select FilePath from department Where FilePath != '' and StoreId=@StoreId Union All Select CategoryImage from AddCategory Where CategoryImage != '' and StoreId=@StoreId Union All Select CategoryImage from Category Where CategoryImage != '' and StoreId=@StoreId ) as tbl";
                SqlCommand command = new SqlCommand(queryString, conn);
                command.Parameters.AddWithValue("@StoreId", storeId);
                conn.Open();
                SqlDataReader reader = command.ExecuteReader();
                try
                {
                    while (reader.Read())
                    {
                        if (!System.IO.File.Exists(basePathPOS + "\\wwwroot\\assets\\Icon\\" + reader["FilePath"]))
                        {
                            using (var client = new System.Net.WebClient())
                            {
                                if (UrlFileExists(url + reader["FilePath"]))
                                {
                                    client.DownloadFile(url + reader["FilePath"], basePathPOS + "\\wwwroot\\assets\\Icon\\" + reader["FilePath"]);
                                }
                            }
                        }
                    }
                    reader.Close();
                    conn.Close();
                }
                catch (Exception ex)
                {
                    reader.Close(); conn.Close();
                    SendErrorToText(ex, "copyFile Loop");
                }
            }
            catch (Exception ex)
            {
                SendErrorToText(ex, "copyFile");
            }
        }
        static bool UrlFileExists(string url)
        {
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.Method = "HEAD";

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    if (response.ContentLength > 1483)
                        return response.StatusCode == HttpStatusCode.OK;
                    else
                        return false;
                }
            }
            catch (WebException)
            {
                return false;
            }
        }
        public void DownLoadZip()
        {
            try
            {
                var basePathPOS = basePath + "POS";
                if (!Directory.Exists(basePathPOS))
                    Directory.CreateDirectory(basePathPOS);
                var urlpath = url + ZipFileName;
                if (UrlFileExists(urlpath))
                {
                    var client = new System.Net.WebClient();
                    client.DownloadFile(urlpath, basePathPOS + "\\" + ZipFileName);
                }
            }
            catch (Exception e)
            {
                SendErrorToText(e, "DownLoadZip");
            }
        }
        public void ExtracZip()
        {
            try
            {
                var basePathPOS = basePath + "POS";
                string zipPath = basePathPOS + "\\" + ZipFileName;
                string extractPath = basePathPOS;
                if (System.IO.File.Exists(zipPath))
                {
                    // Extracts all files to the specified directory
                    System.IO.DirectoryInfo myDirInfo = new DirectoryInfo(extractPath);

                    foreach (FileInfo file in myDirInfo.GetFiles())
                    {
                        if (file.Name != ZipFileName)
                        {
                            file.Delete();
                        }
                    }
                    foreach (DirectoryInfo dir in myDirInfo.GetDirectories())
                    {
                        dir.Delete(true);
                    }
                    ZipFile.ExtractToDirectory(zipPath, extractPath);
                    Console.WriteLine("Extraction completed successfully.");
                    System.IO.File.Delete(zipPath);
                }
            }
            catch (Exception ex)
            {
                SendErrorToText(ex, "ExtracZip");
            }
        }
        public class Transaction
        {
            public string TransactionId { get; set; }
            public string EventEndDate { get; set; }
            public string EventEndTime { get; set; }
            public string TransactionTotalGrossAmount { get; set; }
            public string TransactionTotalTaxNetAmount { get; set; }
            public string TransactionTotalGrandAmount { get; set; }
            public string StoreId { get; set; }
            public string POSId { get; set; }
        }
        public class Tender
        {
            public string Status { get; set; }
            public string TenderCode { get; set; }
            public string TenderSubCode { get; set; }
            public string TenderAmount { get; set; }
            public string Value { get; set; }
            public string TenderInfo_Id { get; set; }
            public string VoucherType { get; set; }
            public string AccountName { get; set; }
        }
        public class Item
        {
            public string Status { get; set; }
            public string Format { get; set; }
            public string POSCode { get; set; }
            public string Descripation { get; set; }
            public string Method { get; set; }
            public string ActualSalesPrice { get; set; }
            public string MerchandiseCode { get; set; }
            public string SellingUnits { get; set; }
            public string RegularSellPrice { get; set; }
            public string SalesQuantity { get; set; }
            public string SalesAmount { get; set; }
            public string TaxLevelld { get; set; }
            public string ItemLine_Id { get; set; }
            public string VoucherType { get; set; }
            public string PromotionID { get; set; }
            public string PromotionAmount { get; set; }
            public string DiscountAmount { get; set; }
            public string CustomerLoyaltyId { get; set; }
            public string LoyaltyProgramName { get; set; }
            public string LoyaltyEntryMethod { get; set; }
            public string LoyaltyDiscountAmount { get; set; }

        }
    }
}

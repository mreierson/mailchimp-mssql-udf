using MailChimp;
using MailChimp.Helper;
using MailChimp.Lists;
using Microsoft.SqlServer.Server;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;

public class Programmability
{

    #region ResultsMetaData

    static SqlMetaData[] CompleteResultsMetaData = {
        new SqlMetaData("complete", SqlDbType.Bit)
    };

    static SqlMetaData[] EmailParameterResultsMetaData = {
        new SqlMetaData("email", SqlDbType.NVarChar, 255),
        new SqlMetaData("euid", SqlDbType.NVarChar, 255),
        new SqlMetaData("leid", SqlDbType.NVarChar, 255)
    };

    static SqlMetaData[] IdResultsMetaData = {
        new SqlMetaData("id", SqlDbType.Int)
    };

    static SqlMetaData[] MergeVarResultsMetaData = {
        new SqlMetaData("name", SqlDbType.NVarChar, 255),
        new SqlMetaData("req", SqlDbType.Bit),
        new SqlMetaData("field_type", SqlDbType.NVarChar, 255),
        new SqlMetaData("public", SqlDbType.Bit),
        new SqlMetaData("show", SqlDbType.Bit),
        new SqlMetaData("order", SqlDbType.Int),
        new SqlMetaData("default_value", SqlDbType.NVarChar, 255),
        new SqlMetaData("helptext", SqlDbType.NVarChar, 255),
        new SqlMetaData("choices", SqlDbType.NVarChar, 255),
        new SqlMetaData("size", SqlDbType.NVarChar, 255),
        new SqlMetaData("tag", SqlDbType.NVarChar, 255),
        new SqlMetaData("id", SqlDbType.Int)
    };

    #endregion

    #region ResultsRows

    private class MemberMergeVarRow
    {
        public string ListId { get; set; }
        public string Email { get; set; }
        public string EUId { get; set; }
        public int LEId { get; set; }
        public string MergeVarName { get; set; }
        public string MergeVarValue { get; set; }
    }

    private class InterestGroupingRow
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string GroupName { get; set; }
        public int DisplayOrder { get; set; }
        public int Subscribers { get; set; }
    }

    private class MemberInterestGroupRow
    {
        public int? Id { get; set; }
        public string Name { get; set; }
        public string GroupNames { get; set; }
        public string GroupInterestName { get; set; }
        public bool Interested { get; set; }
    }

    private class SegmentRow
    {
        public string Type { get; set;}
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime? CreatedDate { get; set; }
        public DateTime? LastUpdate { get; set; }
        public DateTime? LastReset { get; set; }
        public string SegmentOpts { get; set; }
        public string SegmentText { get; set; }
    }

    #endregion

    #region FillRowMethods


    #endregion

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsAbuseReports_FillRow",
        TableDefinition = "[date] datetime, [email] nvarchar(255), [campaign_id] nvarchar(255), [type] nvarchar(255)")]
    public static IEnumerable ListsAbuseReports(SqlString apikey, SqlString list_id)
    {
        string cListId = list_id.ToString();

        MailChimpManager mc = new MailChimpManager(apikey.ToString());

        AbuseResult abuseResult = mc.GetListAbuseReports(cListId, 0, 500, "");

        List<AbuseReport> reports = new List<AbuseReport>(abuseResult.Total);
        reports.AddRange(abuseResult.Data);

        int page = 1;
        int total = abuseResult.Total;
        int currentTotal = abuseResult.Data.Count;

        while (currentTotal < total)
        {
            abuseResult = mc.GetListAbuseReports(cListId, page++, 500, "");
            reports.AddRange(abuseResult.Data);
            currentTotal += abuseResult.Data.Count;
        }
        return reports;
    }
    public static void ListsAbuseReports_FillRow(object resultObj, out SqlDateTime date, out SqlString email, out SqlString campaignId, out SqlString type)
    {
        AbuseReport abuseReport = (AbuseReport)resultObj;
        date = DateTime.Parse(abuseReport.Date);
        email = abuseReport.Email;
        campaignId = abuseReport.CampaignId;
        type = abuseReport.Type;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsActivity_FillRow",
        TableDefinition = @"[user_id] nvarchar(255), [date] datetime, [emails_sent] int, [unique_opens] int, [recipient_clicks] int, [hard_bounce] int,
            [soft_bounce] int, [abuse_reports] int, [subs] int, [unsubs] int, [other_adds] int, [other_removes] int")]
    public static IEnumerable ListsActivity(SqlString apikey, SqlString list_id)
    {
        MailChimpManager mc = new MailChimpManager(apikey.ToString());
        return mc.GetListActivity(list_id.ToString());
    }

    public static void ListsActivity_FillRow(object resultObj, out SqlString userId, out SqlDateTime date, out Int32 emailsSent, out Int32 uniqueOpens,
        out Int32 recipientClicks, out Int32 hardBounce, out Int32 softBounce, out Int32 abuseReports, out Int32 subs, out Int32 unsubs,
        out Int32 otherAdds, out Int32 otherRemoves)
    {
        ListActivity listActivity = (ListActivity)resultObj;
        userId = listActivity.UserId;
        date = DateTime.Parse(listActivity.Date);
        emailsSent = listActivity.EmailSent;
        uniqueOpens = listActivity.UniqueOpens;
        recipientClicks = listActivity.RecipientClicks;
        hardBounce = listActivity.HardBounce;
        softBounce = listActivity.SoftBounce;
        abuseReports = listActivity.AbuseReports;
        subs = listActivity.Subscriptions;
        unsubs = listActivity.Unsubscriptions;
        otherAdds = listActivity.OtherAdds;
        otherRemoves = listActivity.OtherRemoves;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsInterestGroupAdd(SqlString apikey, SqlString list_id, SqlString group_name, SqlInt32 grouping_id)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            CompleteResult result = mc.AddListInterestGroup(list_id.ToString(), group_name.ToString(), grouping_id.Value);

            SqlDataRecord record = new SqlDataRecord(CompleteResultsMetaData);
            record.SetBoolean(0, result.Complete);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsInterestGroupDel(SqlString apikey, SqlString list_id, SqlString group_name, SqlInt32 grouping_id)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            CompleteResult result = mc.DeleteListInterestGroup(list_id.ToString(), group_name.ToString(), grouping_id.Value);

            SqlDataRecord record = new SqlDataRecord(CompleteResultsMetaData);
            record.SetBoolean(0, result.Complete);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsInterestGroupUpdate(SqlString apikey, SqlString list_id, SqlString old_name, SqlString new_name, SqlInt32 grouping_id)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            CompleteResult result = mc.UpdateListInterestGroup(list_id.ToString(), old_name.ToString(), new_name.ToString(), grouping_id.Value);

            SqlDataRecord record = new SqlDataRecord(CompleteResultsMetaData);
            record.SetBoolean(0, result.Complete);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsInterestGroupingAdd(SqlString apikey, SqlString list_id, SqlString name, SqlString type, SqlString groupsCSV)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            string[] groups = groupsCSV.ToString().Split(',');

            InterestGroupingResult result = mc.AddListInterestGrouping(list_id.ToString(), name.ToString(), type.ToString(), new List<string>(groups));

            SqlDataRecord record = new SqlDataRecord(IdResultsMetaData);
            record.SetInt32(0, result.id);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsInterestGroupingDel(SqlString apikey, SqlString list_id, SqlInt32 grouping_id)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            CompleteResult result = mc.DeleteListInterestGrouping(list_id.ToString(), grouping_id.Value);

            SqlDataRecord record = new SqlDataRecord(CompleteResultsMetaData);
            record.SetBoolean(0, result.Complete);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsInterestGroupingUpdate(SqlString apikey, SqlString list_id, SqlInt32 grouping_id, SqlString name, SqlString value)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            CompleteResult result = mc.UpdateListInterestGrouping(list_id.ToString(), grouping_id.Value, name.ToString(), value.ToString());

            SqlDataRecord record = new SqlDataRecord(CompleteResultsMetaData);
            record.SetBoolean(0, result.Complete);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsInterestGroupings_FillRow",
        TableDefinition = @"[id] int, [name] nvarchar(255), [group_name] nvarchar(255), [display_order] int, [subscribers] int")]
    public static IEnumerable ListsInterestGroupings(SqlString apikey, SqlString list_id, SqlBoolean counts)
    {
        List<InterestGroupingRow> rows = new List<InterestGroupingRow>();

        MailChimpManager mc = new MailChimpManager(apikey.ToString());

        List<InterestGrouping> results = mc.GetListInterestGroupings(list_id.ToString(), counts.IsTrue);

        foreach (InterestGrouping grouping in results)
        {
            foreach (InterestGrouping.InnerGroup innerGroup in grouping.GroupNames)
            {
                InterestGroupingRow row = new InterestGroupingRow();
                row.Id = grouping.Id;
                row.Name = grouping.Name;
                row.GroupName = innerGroup.Name;
                row.DisplayOrder = innerGroup.DisplayOrder;
                row.Subscribers = innerGroup.Subscribers;
                rows.Add(row);
            }
        }

        return rows;
    }
    public static void ListsInterestGroupings_FillRow(object resultObj, out SqlInt32 id, out SqlString name, out SqlString group_name, 
        out SqlInt32 display_order, out SqlInt32 subscribers)
    {
        InterestGroupingRow row = (InterestGroupingRow)resultObj;
        id = row.Id;
        name = row.Name;
        group_name = row.GroupName;
        display_order = row.DisplayOrder;
        subscribers = row.Subscribers;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsList_FillRow",
        TableDefinition = @"[id] nvarchar(255), [name] nvarchar(255), [date_created] datetime, [email_type_option] bit, [use_awesomebar] bit, 
            [default_from_name] nvarchar(255), [default_from_email] nvarchar(255), [default_subject] nvarchar(255), [default_language] nvarchar(255),
            [list_rating] float, [subscribe_url_short] nvarchar(255), [subscribe_url_long] nvarchar(255), [beamer_address] nvarchar(255), [visibility] nvarchar(255)")]
    public static IEnumerable ListsList(SqlString apikey)
    {
        MailChimpManager mc = new MailChimpManager(apikey.ToString());

        ListResult listResult = mc.GetLists(null, 0, 100, "created", "DESC");

        List<ListInfo> lists = new List<ListInfo>(listResult.Total);
        lists.AddRange(listResult.Data);

        int page = 1;
        int total = listResult.Total;
        int currentTotal = listResult.Data.Count;

        while (currentTotal < total)
        {
            listResult = mc.GetLists(null, page++, 100, "created", "DESC");
            lists.AddRange(listResult.Data);
            currentTotal += listResult.Data.Count;
        }
        return lists;
    }

    public static void ListsList_FillRow(object resultObj, out SqlString id, out SqlString name, out SqlDateTime dateCreated, out SqlBoolean emailTypeOption,
        out SqlBoolean useAwesomeBar, out SqlString defaultFromName, out SqlString defaultFromEmail, out SqlString defaultSubject, out SqlString defaultLanguage,
        out SqlDouble listRating, out SqlString subscriberUrlShort, out SqlString subscribeUrlLong, out SqlString beamerAddress, out SqlString visibility)
    {
        ListInfo listInfo = (ListInfo)resultObj;
        id = listInfo.Id;
        name = listInfo.Name;
        dateCreated = DateTime.Parse(listInfo.DateCreated);
        emailTypeOption = listInfo.EmailTypeOption ? SqlBoolean.True : SqlBoolean.False;
        useAwesomeBar = listInfo.UseAwesomebar ? SqlBoolean.True : SqlBoolean.False;
        defaultFromName = listInfo.DefaultFromName;
        defaultFromEmail = listInfo.DefaultFromEmail;
        defaultSubject = listInfo.DefaultSubject;
        defaultLanguage = listInfo.DefaultLanguage;
        listRating = listInfo.ListRating;
        subscriberUrlShort = listInfo.SubscribeUrlShort;
        subscribeUrlLong = listInfo.SubscribeUrlLong;
        beamerAddress = listInfo.BeamerAddress;
        visibility = listInfo.Visibility;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsMembers_FillRow",
        TableDefinition = @"[euid] nvarchar(255), [email] nvarchar(255), [email_type] nvarchar(255), [status] nvarchar(255),
            [ip_signup] nvarchar(255), [timestamp_signup] datetime, [ip_opt] nvarchar(255), [timestamp_opt] datetime, [member_rating] int,
            [campaign_id] nvarchar(255), [timestamp] datetime, [info_changed] datetime, [web_id] int, [leid] int, [list_id] nvarchar(255),
            [list_name] nvarchar(255), [language] nvarchar(255), [is_gmonkey] bit")]
    public static IEnumerable ListsMembers(SqlString apikey, SqlString list_id, SqlString status)
    {
        string cListId = list_id.ToString();
        string cStatus = status.ToString();

        MailChimpManager mc = new MailChimpManager(apikey.ToString());

        MembersResult membersResult = mc.GetAllMembersForList(cListId, cStatus, 0, 100, "", "ASC", null);

        List<MemberInfo> members = new List<MemberInfo>(membersResult.Total);
        members.AddRange(membersResult.Data);

        int page = 1;
        int total = membersResult.Total;
        int currentTotal = membersResult.Data.Count;

        while (currentTotal < total)
        {
            membersResult = mc.GetAllMembersForList(cListId, cStatus, page++, 100, "", "ASC", null);
            members.AddRange(membersResult.Data);
            currentTotal += membersResult.Data.Count;
        }
        return members;
    }

    public static void ListsMembers_FillRow(object resultObj, out SqlString id, out SqlString email, out SqlString emailType, out SqlString status,
        out SqlString ipSignup, out SqlDateTime timestampSignup, out SqlString ipOpt, out SqlDateTime timestampOpt, out Int32 memberRating,
        out SqlString campaignId, out SqlDateTime timestamp, out SqlDateTime infoChanged, out Int32 webId, out Int32 leid, out SqlString listId,
        out SqlString listName, out SqlString language, out SqlBoolean isGmonkey)
    {
        MemberInfo memberInfo = (MemberInfo)resultObj;
        id = memberInfo.Id;
        email = memberInfo.Email;
        emailType = memberInfo.EmailType;
        status = memberInfo.Status;
        ipSignup = memberInfo.IPSignup;
        timestampSignup = memberInfo.TimestampSignup ?? SqlDateTime.Null;
        ipOpt = memberInfo.IPOptIn;
        timestampOpt = memberInfo.TimestampOptIn ?? SqlDateTime.Null;
        memberRating = memberInfo.MemberRating;
        campaignId = memberInfo.CampaignId;
        timestamp = memberInfo.Timestamp ?? SqlDateTime.Null;
        infoChanged = memberInfo.InfoChanged ?? SqlDateTime.Null;
        webId = memberInfo.WebId;
        leid = memberInfo.LEId;
        listId = memberInfo.ListId;
        listName = memberInfo.ListName;
        language = memberInfo.Language;
        isGmonkey = memberInfo.IsGoldenMonkey ? SqlBoolean.True : SqlBoolean.False;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsMemberInterestGroupings_FillRow",
        TableDefinition = @"[id] int, [name] nvarchar(255), [group_names] nvarchar(255), [group_interest_name] nvarchar(255), [interested] bit")]
    public static IEnumerable ListsMemberInterestGroupings(SqlString apikey, SqlString list_id, SqlString email, SqlString euid, SqlString leid)
    {
        List<MemberInterestGroupRow> rows = new List<MemberInterestGroupRow>();

        string cListId = list_id.ToString();

        MailChimpManager mc = new MailChimpManager(apikey.ToString());

        MemberInfoResult memberResult = mc.GetMemberInfo(cListId, new List<EmailParameter>
        {
            new EmailParameter {
                Email = email.ToString(),
                EUId = euid.ToString(),
                LEId = leid.ToString()
            }   
        });

        if (memberResult.SuccessCount > 0)
        {
            MemberInfo memberInfo = memberResult.Data[0];

            if (memberInfo.MemberMergeInfo.Groupings != null)
            {
                foreach (Grouping grouping in memberInfo.MemberMergeInfo.Groupings)
                {
                    string groupNames = (grouping.GroupNames == null) ? string.Empty : string.Join(",", grouping.GroupNames.ToArray());

                    foreach (MailChimp.Lists.Grouping.GroupInterest interest in grouping.GroupInterests)
                    {

                        MemberInterestGroupRow row = new MemberInterestGroupRow
                        {
                            Id = grouping.Id,
                            Name = grouping.Name,
                            GroupNames = groupNames,
                            GroupInterestName = interest.Name,
                            Interested = interest.Interested
                        };
                        rows.Add(row);
                    }
                }
            }
        }
        return rows;
    }

    public static void ListsMemberInterestGroupings_FillRow(object resultObj, out SqlInt32 id, out SqlString name, out SqlString group_names, 
        out SqlString group_interest_name, out SqlBoolean interested)
    {
        MemberInterestGroupRow row = (MemberInterestGroupRow)resultObj;
        id = row.Id ?? SqlInt32.Null;
        name = row.Name;
        group_names = row.GroupNames;
        group_interest_name = row.GroupInterestName;
        interested = row.Interested;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsMemberMergeVars_FillRow",
        TableDefinition = @"[list_id] nvarchar(255), [email] nvarchar(255), [leid] int, [euid] nvarchar(255), [merge_var_name] nvarchar(255), [merge_var_value] nvarchar(255)")]
    public static IEnumerable ListsMemberMergeVars(SqlString apikey, SqlString list_id, SqlString email, SqlString euid, SqlString leid)
    {
        List<MemberMergeVarRow> vars = new List<MemberMergeVarRow>();

        string cListId = list_id.ToString();

        MailChimpManager mc = new MailChimpManager(apikey.ToString());

        MemberInfoResult memberResult = mc.GetMemberInfo(cListId, new List<EmailParameter>
        {
            new EmailParameter {
                Email = email.ToString(),
                EUId = euid.ToString(),
                LEId = leid.ToString()
            }   
        });

        if (memberResult.SuccessCount > 0)
        {
            MemberInfo memberInfo = memberResult.Data[0];
            foreach (string key in memberInfo.MemberMergeInfo.Keys)
            {
                object value = memberInfo.MemberMergeInfo[key];
                vars.Add(new MemberMergeVarRow
                {
                    ListId = cListId,
                    Email = memberInfo.Email,
                    LEId = memberInfo.LEId,
                    EUId = memberInfo.Id,
                    MergeVarName = key,
                    MergeVarValue = (value ?? string.Empty).ToString()
                });

            }
        }
        return vars;
    }

    public static void ListsMemberMergeVars_FillRow(object resultObj, out SqlString list_id, out SqlString email, out Int32 leid, out SqlString euid,
        out SqlString merge_var_name, out SqlString merge_var_value)
    {
        MemberMergeVarRow mergeVar = (MemberMergeVarRow)resultObj;
        list_id = mergeVar.ListId;
        email = mergeVar.Email;
        leid = mergeVar.LEId;
        euid = mergeVar.EUId;
        merge_var_name = mergeVar.MergeVarName;
        merge_var_value = mergeVar.MergeVarValue;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsMemberMergeVarSet(SqlString apikey, SqlString list_id, SqlString email, SqlString euid, SqlString leid, SqlString tag, SqlString value)
    {
        try
        {
            string cListId = list_id.ToString();

            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            EmailParameter emailParam = new EmailParameter {
                Email = email.ToString(),
                EUId = euid.ToString(),
                LEId = leid.ToString()
            };

            MemberInfoResult memberResult = mc.GetMemberInfo(cListId, new List<EmailParameter> { emailParam });

            if (memberResult.SuccessCount > 0)
            {
                MemberInfo memberInfo = memberResult.Data[0];
                memberInfo.MemberMergeInfo[tag.ToString()] = value.ToString();
                mc.UpdateMember(cListId, emailParam, memberInfo.MemberMergeInfo, "", true);
            }
            else
            {
                SqlContext.Pipe.Send(String.Format("Member not found : email = {0}, euid = {1}, leid = {2}",
                    email.ToString(), euid.ToString(), leid.ToString()));
                return 1;
            }
        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsMemberNotes_FillRow",
        TableDefinition = @"[id] int, [note] nvarchar(255), [created] datetime, [updated] datetime, [created_by_name] nvarchar(255)")]
    public static IEnumerable ListsMemberNotes(SqlString apikey, SqlString list_id, SqlString email, SqlString euid, SqlString leid)
    {
        List<MemberNote> rows = new List<MemberNote>();

        string cListId = list_id.ToString();

        MailChimpManager mc = new MailChimpManager(apikey.ToString());

        MemberInfoResult memberResult = mc.GetMemberInfo(cListId, new List<EmailParameter>
        {
            new EmailParameter {
                Email = email.ToString(),
                EUId = euid.ToString(),
                LEId = leid.ToString()
            }   
        });

        if (memberResult.SuccessCount > 0)
        {
            MemberInfo memberInfo = memberResult.Data[0];
            rows.AddRange(memberInfo.Notes);
        }
        return rows;
    }

    public static void ListsMemberNotes_FillRow(object resultObj, out SqlInt32 id, out SqlString note, out SqlDateTime created, out SqlDateTime updated,
        out SqlString created_by_name)
    {
        MemberNote row = (MemberNote)resultObj;
        id = row.Id;
        note = row.Note;
        created = DateTime.Parse(row.Created);
        updated = DateTime.Parse(row.Updated);
        created_by_name = row.CreatedByName;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsMergeVarAdd(SqlString apikey, SqlString list_id, SqlString tag, SqlString name,  SqlString opt_field_type, SqlBoolean opt_req,
        SqlBoolean opt_public, SqlBoolean opt_show, SqlInt32 opt_order, SqlString opt_default_value, SqlString opt_helptext, SqlString opt_choices,
        SqlString opt_dateformat, SqlString opt_phoneformat, SqlString opt_defaultcountry)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            MergeVarItemResult result = mc.AddMergeVar(list_id.ToString(), tag.ToString(), name.ToString(), new MergeVarOptions
            {
                FieldType = opt_field_type.ToString(),
                Required = opt_req.IsTrue,
                Public = opt_public.IsTrue,
                Show = opt_show.IsTrue,
                Order = opt_order.Value,
                DefaultValue = opt_default_value.ToString(),
                HelpText = opt_helptext.ToString(),
                Choices = opt_choices.ToString().Split(','),
                DateFormat = opt_dateformat.ToString(),
                PhoneFormat = opt_phoneformat.ToString(),
                DefaultCountry = opt_defaultcountry.ToString()

            });

            SqlDataRecord record = new SqlDataRecord(MergeVarResultsMetaData);
            record.SetString(0, result.Name);
            record.SetBoolean(1, result.Required);
            record.SetString(2, result.FieldType);
            record.SetBoolean(3, result.Public);
            record.SetBoolean(4, result.Show);
            record.SetInt32(5, result.Order);
            record.SetString(6, result.DefaultValue);
            record.SetString(7, result.HelpText);
            record.SetString(8, (result.Choices == null) ? string.Empty : string.Join(",", result.Choices));
            record.SetString(9, result.Size);
            record.SetString(10, result.Tag);
            record.SetInt32(11, result.Id);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            SqlContext.Pipe.Send(ex.StackTrace);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsMergeVarDel(SqlString apikey, SqlString list_id, SqlString tag)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            CompleteResult result = mc.DeleteMergeVar(list_id.ToString(), tag.ToString());

            SqlDataRecord record = new SqlDataRecord(CompleteResultsMetaData);
            record.SetBoolean(0, result.Complete);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsMergeVarReset(SqlString apikey, SqlString list_id, SqlString tag)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            CompleteResult result = mc.ResetMergeVar(list_id.ToString(), tag.ToString());

            SqlDataRecord record = new SqlDataRecord(CompleteResultsMetaData);
            record.SetBoolean(0, result.Complete);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }


    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsMergeVarSet(SqlString apikey, SqlString list_id, SqlString tag, SqlString value)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            CompleteResult result = mc.SetMergeVar(list_id.ToString(), tag.ToString(), value.ToString());

            SqlDataRecord record = new SqlDataRecord(CompleteResultsMetaData);
            record.SetBoolean(0, result.Complete);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsMergeVarUpdate(SqlString apikey, SqlString list_id, SqlString tag, SqlBoolean opt_req, SqlBoolean opt_public, SqlBoolean opt_show, 
        SqlInt32 opt_order, SqlString opt_default_value, SqlString opt_helptext, SqlString opt_choices, SqlString opt_dateformat, 
        SqlString opt_phoneformat, SqlString opt_defaultcountry)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            MergeVarItemResult result = mc.UpdateMergeVar(list_id.ToString(), tag.ToString(), new MergeVarOptions
            {
                Required = opt_req.IsTrue,
                Public = opt_public.IsTrue,
                Show = opt_show.IsTrue,
                Order = opt_order.Value,
                DefaultValue = opt_default_value.ToString(),
                HelpText = opt_helptext.ToString(),
                Choices = opt_choices.ToString().Split(','),
                DateFormat = opt_dateformat.ToString(),
                PhoneFormat = opt_phoneformat.ToString(),
                DefaultCountry = opt_defaultcountry.ToString()

            });

            SqlDataRecord record = new SqlDataRecord(MergeVarResultsMetaData);
            record.SetString(0, result.Name);
            record.SetBoolean(1, result.Required);
            record.SetString(2, result.FieldType);
            record.SetBoolean(3, result.Public);
            record.SetBoolean(4, result.Show);
            record.SetInt32(5, result.Order);
            record.SetString(6, result.DefaultValue);
            record.SetString(7, result.HelpText);
            record.SetString(8, (result.Choices == null) ? string.Empty : string.Join(",", result.Choices));
            record.SetString(9, result.Size);
            record.SetString(10, result.Tag);
            record.SetInt32(11, result.Id);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            SqlContext.Pipe.Send(ex.StackTrace);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsMergeVars_FillRow",
        TableDefinition = @"[name] nvarchar(255), [req] bit, [field_type] nvarchar(255), [public] bit, [show] bit, [order] int, 
            [default_value] nvarchar(255), [helptext] nvarchar(255), [size] nvarchar(255), [tag] nvarchar(255), [choices] nvarchar(255), [id] int")]
    public static IEnumerable ListsMergeVars(SqlString apikey, SqlString list_id)
    {
        List<MergeVarItemResult> vars = new List<MergeVarItemResult>();

        MailChimpManager mc = new MailChimpManager(apikey.ToString());
        MergeVarResult result = mc.GetMergeVars(new List<String>{ list_id.ToString() });
        if (result.Data.Count > 0)
        {
            vars.AddRange(result.Data[0].MergeVars);
        }

        return vars;
    }

    public static void ListsMergeVars_FillRow(object resultObj,  out SqlString name, out SqlBoolean req,
        out SqlString field_type, out SqlBoolean is_public, out SqlBoolean show, out SqlInt32 order, out SqlString default_value,
        out SqlString helptext, out SqlString size, out SqlString tag, out SqlString choices, out SqlInt32 id)
    {
        MergeVarItemResult itemResult = (MergeVarItemResult) resultObj;
        name = itemResult.Name;
        req = itemResult.Required ? SqlBoolean.True : SqlBoolean.False;
        field_type = itemResult.FieldType;
        is_public = itemResult.Public ? SqlBoolean.True : SqlBoolean.False;
        show = itemResult.Show ? SqlBoolean.True : SqlBoolean.False;
        order = itemResult.Order;
        default_value = itemResult.DefaultValue;
        helptext = itemResult.HelpText;
        size = itemResult.Size;
        tag = itemResult.Tag;
        choices = (itemResult.Choices == null) ? string.Empty : string.Join(",", itemResult.Choices);
        id = itemResult.Id;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, FillRowMethodName = "ListsSegments_FillRow",
        TableDefinition = @"[type] nvarchar(255), [id] int, [name] nvarchar(255), [segment_opts] nvarchar(255), [segment_text] nvarchar(255),
            [created_date] datetime, [last_update] datetime, [last_reset] datetime")]
    public static IEnumerable ListsSegments(SqlString apikey, SqlString list_id, SqlString segment_type)
    {
        List<SegmentRow> rows = new List<SegmentRow>();

        MailChimpManager mc = new MailChimpManager(apikey.ToString());

        SegmentResult result = mc.GetSegmentsForList(list_id.ToString(), segment_type.ToString());
        
        if (result.SavedResult != null)
        {
            foreach (SavedSegmentResult savedResult in result.SavedResult)
            {
                rows.Add(new SegmentRow
                {
                    Type = segment_type.ToString(),
                    Id = savedResult.Id,
                    Name = savedResult.Name,
                    SegmentOpts = savedResult.SegmentOpts,
                    SegmentText = savedResult.SegmentText
                });
            }
        }

        if (result.StaticResult != null)
        {
            foreach (StaticSegmentResult staticResult in result.StaticResult)
            {
                rows.Add(new SegmentRow
                {
                    Type = segment_type.ToString(),
                    Id = staticResult.StaticSegmentId,
                    Name = staticResult.SegmentName,
                    CreatedDate = staticResult.createdDate,
                    LastUpdate = staticResult.lastUpdate,
                    LastReset = staticResult.lastReset
                });
            }
        }

        return rows;
    }

    public static void ListsSegments_FillRow(object resultObj, out SqlString type, out SqlInt32 id, out SqlString name, out SqlString segment_opts,
        out SqlString segment_text, out SqlDateTime created_date, out SqlDateTime last_update, out SqlDateTime last_reset)
    {
        SegmentRow row = (SegmentRow)resultObj;
        type = row.Type;
        id = row.Id;
        name = row.Name;
        segment_opts = row.SegmentOpts;
        segment_text = row.SegmentText;
        created_date = row.CreatedDate ?? SqlDateTime.Null;
        last_update = row.LastUpdate ?? SqlDateTime.Null;
        last_reset = row.LastReset ?? SqlDateTime.Null;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsSubscribe(SqlString apikey, SqlString list_id, SqlString email, SqlString euid, SqlString leid, SqlString email_type, SqlBoolean double_optin, SqlBoolean update_existing, SqlBoolean send_welcome)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            EmailParameter emailParam = new EmailParameter
            {
                Email = email.ToString(),
                EUId = euid.ToString(),
                LEId = leid.ToString()
            };

            EmailParameter result = mc.Subscribe(list_id.ToString(), emailParam, null, email_type.ToString(),
                double_optin.IsTrue, update_existing.IsTrue, true, send_welcome.IsTrue);

            SqlDataRecord record = new SqlDataRecord(EmailParameterResultsMetaData);
            record.SetString(0, result.Email);
            record.SetString(1, result.EUId);
            record.SetString(2, result.LEId);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int ListsUnsubscribe(SqlString apikey, SqlString list_id, SqlString email, SqlString euid, SqlString leid, SqlBoolean delete_member, SqlBoolean send_goodbye, SqlBoolean send_notify)
    {
        try
        {
            MailChimpManager mc = new MailChimpManager(apikey.ToString());

            EmailParameter emailParam = new EmailParameter
            {
                Email = email.ToString(),
                EUId = euid.ToString(),
                LEId = leid.ToString()
            };

            UnsubscribeResult result = mc.Unsubscribe(list_id.ToString(), emailParam, delete_member.IsTrue, send_goodbye.IsTrue, send_notify.IsTrue);

            SqlDataRecord record = new SqlDataRecord(CompleteResultsMetaData);
            record.SetBoolean(0, result.Complete);

            SqlContext.Pipe.Send(record);

        }
        catch (Exception ex)
        {
            SqlContext.Pipe.Send(ex.Message);
            return 1;
        }
        return 0;
    }
}
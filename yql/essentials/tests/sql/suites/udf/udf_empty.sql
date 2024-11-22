/* postgres can not */
select SimpleUdf::ReturnNull(""), SimpleUdf::ReturnVoid(""), SimpleUdf::ReturnEmpty(""), SimpleUdf::ReturnEmpty("") is null;

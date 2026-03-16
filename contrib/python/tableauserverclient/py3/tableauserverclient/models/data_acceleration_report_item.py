from defusedxml.ElementTree import fromstring


class DataAccelerationReportItem:
    class ComparisonRecord:
        def __init__(
            self,
            site,
            sheet_uri,
            unaccelerated_session_count,
            avg_non_accelerated_plt,
            accelerated_session_count,
            avg_accelerated_plt,
        ):
            self._site = site
            self._sheet_uri = sheet_uri
            self._unaccelerated_session_count = unaccelerated_session_count
            self._avg_non_accelerated_plt = avg_non_accelerated_plt
            self._accelerated_session_count = accelerated_session_count
            self._avg_accelerated_plt = avg_accelerated_plt

        @property
        def site(self):
            return self._site

        @property
        def sheet_uri(self):
            return self._sheet_uri

        @property
        def unaccelerated_session_count(self):
            return self._unaccelerated_session_count

        @property
        def accelerated_session_count(self):
            return self._accelerated_session_count

        @property
        def avg_accelerated_plt(self):
            return self._avg_accelerated_plt

        @property
        def avg_non_accelerated_plt(self):
            return self._avg_non_accelerated_plt

    def __init__(self, comparison_records):
        self._comparison_records = comparison_records

    def __repr__(self):
        return f"<(deprecated)DataAccelerationReportItem site={self.site} sheet={sheet_uri}>"

    @property
    def comparison_records(self):
        return self._comparison_records

    @staticmethod
    def _parse_element(comparison_record_xml, ns):
        site = comparison_record_xml.get("site", None)
        sheet_uri = comparison_record_xml.get("sheetURI", None)
        unaccelerated_session_count = comparison_record_xml.get("unacceleratedSessionCount", None)
        avg_non_accelerated_plt = comparison_record_xml.get("averageNonAcceleratedPLT", None)
        accelerated_session_count = comparison_record_xml.get("acceleratedSessionCount", None)
        avg_accelerated_plt = comparison_record_xml.get("averageAcceleratedPLT", None)
        return (
            site,
            sheet_uri,
            unaccelerated_session_count,
            avg_non_accelerated_plt,
            accelerated_session_count,
            avg_accelerated_plt,
        )

    @classmethod
    def from_response(cls, resp, ns):
        comparison_records = list()
        parsed_response = fromstring(resp)
        all_comparison_records_xml = parsed_response.findall(".//t:comparisonRecord", namespaces=ns)
        for comparison_record_xml in all_comparison_records_xml:
            (
                site,
                sheet_uri,
                unaccelerated_session_count,
                avg_non_accelerated_plt,
                accelerated_session_count,
                avg_accelerated_plt,
            ) = cls._parse_element(comparison_record_xml, ns)

            comparison_record = DataAccelerationReportItem.ComparisonRecord(
                site,
                sheet_uri,
                unaccelerated_session_count,
                avg_non_accelerated_plt,
                accelerated_session_count,
                avg_accelerated_plt,
            )
            comparison_records.append(comparison_record)
        return cls(comparison_records)

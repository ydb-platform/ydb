from mpegdash.utils import (
    parse_attr_value, parse_child_nodes, parse_node_value,
    write_attr_value, write_child_node, write_node_value
)


class XMLNode(object):
    def parse(self, xmlnode):
        raise NotImplementedError('Should have implemented this')

    def write(self, xmlnode):
        raise NotImplementedError('Should have implemented this')


class Subset(XMLNode):
    def __init__(self):
        self.id = None                                        # xs:string
        self.contains = []                                    # UIntVectorType (required)

    def parse(self, xmlnode):
        self.id = parse_attr_value(xmlnode, 'id', str)
        self.contains = parse_attr_value(xmlnode, 'contains', [int])

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'id', self.id)
        write_attr_value(xmlnode, 'contains', self.contains)


class URL(XMLNode):
    def __init__(self):
        self.source_url = None                                # xs:anyURI
        self.range = None                                     # xs:string

    def parse(self, xmlnode):
        self.source_url = parse_attr_value(xmlnode, 'sourceURL', str)
        self.range = parse_attr_value(xmlnode, 'range', str)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'sourceURL', self.source_url)
        write_attr_value(xmlnode, 'range', self.range)


class BaseURL(XMLNode):
    def __init__(self):
        self.base_url_value = None                            # xs:anyURI

        self.service_location = None                          # xs:string
        self.byte_range = None                                # xs:string
        self.availability_time_offset = None                  # xs:double
        self.availability_time_complete = None                # xs:boolean

    def parse(self, xmlnode):
        self.base_url_value = parse_node_value(xmlnode, str)

        self.service_location = parse_attr_value(xmlnode, 'serviceLocation', str)
        self.byte_range = parse_attr_value(xmlnode, 'byteRange', str)
        self.availability_time_offset = parse_attr_value(xmlnode, 'availabilityTimeOffset', float)
        self.availability_time_complete = parse_attr_value(xmlnode, 'availabilityTimeComplete', bool)

    def write(self, xmlnode):
        write_node_value(xmlnode, self.base_url_value)

        write_attr_value(xmlnode, 'serviceLocation', self.service_location)
        write_attr_value(xmlnode, 'byteRange', self.byte_range)
        write_attr_value(xmlnode, 'availabilityTimeOffset', self.availability_time_offset)
        write_attr_value(xmlnode, 'availabilityTimeComplete', self.availability_time_complete)


class XsStringElement(XMLNode):
    def __init__(self):
        self.text = None

    def parse(self, xmlnode):
        self.text = parse_node_value(xmlnode, str)

    def write(self, xmlnode):
        write_node_value(xmlnode, self.text)


class ProgramInformation(XMLNode):
    def __init__(self):
        self.lang = None                                      # xs:language
        self.more_information_url = None                      # xs:anyURI

        self.titles = None                                    # xs:string*
        self.sources = None                                   # xs:string*
        self.copyrights = None                                # xs:string*

    def parse(self, xmlnode):
        self.lang = parse_attr_value(xmlnode, 'lang', str)
        self.more_information_url = parse_attr_value(xmlnode, 'moreInformationURL', str)

        self.titles = parse_child_nodes(xmlnode, 'Title', XsStringElement)
        self.sources = parse_child_nodes(xmlnode, 'Source', XsStringElement)
        self.copyrights = parse_child_nodes(xmlnode, 'Copyright', XsStringElement)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'lang', self.lang)
        write_attr_value(xmlnode, 'moreInformationURL', self.more_information_url)

        write_child_node(xmlnode, 'Title', self.titles)
        write_child_node(xmlnode, 'Source', self.sources)
        write_child_node(xmlnode, 'Copyright', self.copyrights)


class Metrics(XMLNode):
    def __init__(self):
        self.metrics = ''                                     # xs:string (required)

        self.reportings = None                                # DescriptorType*
        self.ranges = None                                    # RangeType*

    def parse(self, xmlnode):
        self.metrics = parse_attr_value(xmlnode, 'metrics', str)

        self.reportings = parse_child_nodes(xmlnode, 'Reporting', Descriptor)
        self.ranges = parse_child_nodes(xmlnode, 'Range', Range)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'metrics', self.metrics)

        write_child_node(xmlnode, 'Reporting', self.reportings)
        write_child_node(xmlnode, 'Range', self.ranges)


class Range(XMLNode):
    def __init__(self):
        self.starttime = None                                 # xs:duration
        self.duration = None                                  # xs:duration

    def parse(self, xmlnode):
        self.starttime = parse_attr_value(xmlnode, 'starttime', str)
        self.duration = parse_attr_value(xmlnode, 'duration', str)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'starttime', self.starttime)
        write_attr_value(xmlnode, 'duration', self.duration)


class SegmentURL(XMLNode):
    def __init__(self):
        self.media = None                                     # xs:anyURI
        self.media_range = None                               # xs:string
        self.index = None                                     # xs:anyURI
        self.index_range = None                               # xs:string

    def parse(self, xmlnode):
        self.media = parse_attr_value(xmlnode, 'media', str)
        self.media_range = parse_attr_value(xmlnode, 'mediaRange', str)
        self.index = parse_attr_value(xmlnode, 'index', str)
        self.index_range = parse_attr_value(xmlnode, 'indexRange', str)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'media', self.media)
        write_attr_value(xmlnode, 'mediaRange', self.media_range)
        write_attr_value(xmlnode, 'index', self.index)
        write_attr_value(xmlnode, 'indexRange', self.index_range)


class S(XMLNode):
    def __init__(self):
        self.t = None                                         # xs:unsignedLong
        self.d = 0                                            # xs:unsignedLong (required)
        self.r = None                                         # xml:integer

    def parse(self, xmlnode):
        self.t = parse_attr_value(xmlnode, 't', int)
        self.d = parse_attr_value(xmlnode, 'd', int)
        self.r = parse_attr_value(xmlnode, 'r', int)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 't', self.t)
        write_attr_value(xmlnode, 'd', self.d)
        write_attr_value(xmlnode, 'r', self.r)


class SegmentTimeline(XMLNode):
    def __init__(self):
        self.Ss = None                                        # xs:complexType+

    def parse(self, xmlnode):
        self.Ss = parse_child_nodes(xmlnode, 'S', S)

    def write(self, xmlnode):
        write_child_node(xmlnode, 'S', self.Ss)


class SegmentBase(XMLNode):
    def __init__(self):
        self.timescale = None                                 # xs:unsignedInt
        self.index_range = None                               # xs:string
        self.index_range_exact = None                         # xs:boolean
        self.presentation_time_offset = None                  # xs:unsignedLong
        self.availability_time_offset = None                  # xs:double
        self.availability_time_complete = None                # xs:boolean

        self.initializations = None                           # URLType*
        self.representation_indexes = None                    # URLType*

    def parse(self, xmlnode):
        self.timescale = parse_attr_value(xmlnode, 'timescale', int)
        self.index_range = parse_attr_value(xmlnode, 'indexRange', str)
        self.index_range_exact = parse_attr_value(xmlnode, 'indexRangeExact', bool)
        self.presentation_time_offset = parse_attr_value(xmlnode, 'presentationTimeOffset', int)
        self.availability_time_offset = parse_attr_value(xmlnode, 'availabilityTimeOffset', float)
        self.availability_time_complete = parse_attr_value(xmlnode, 'availabilityTimeComplete', bool)

        self.initializations = parse_child_nodes(xmlnode, 'Initialization', URL)
        self.representation_indexes = parse_child_nodes(xmlnode, 'RepresentationIndex', URL)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'timescale', self.timescale)
        write_attr_value(xmlnode, 'indexRange', self.index_range)
        write_attr_value(xmlnode, 'indexRangeExact', self.index_range_exact)
        write_attr_value(xmlnode, 'presentationTimeOffset', self.presentation_time_offset)
        write_attr_value(xmlnode, 'availabilityTimeOffset', self.availability_time_offset)
        write_attr_value(xmlnode, 'availabilityTimeComplete', self.availability_time_complete)

        write_child_node(xmlnode, 'Initialization', self.initializations)
        write_child_node(xmlnode, 'RepresentationIndex', self.representation_indexes)


class MultipleSegmentBase(SegmentBase):
    def __init__(self):
        SegmentBase.__init__(self)

        self.duration = None                                  # xs:unsignedInt
        self.start_number = None                              # xs:unsignedInt
        self.end_number = None                                # xs:unsignedInt

        self.segment_timelines = None                         # SegmentTimelineType*
        self.bitstream_switchings = None                      # URLType*

    def parse(self, xmlnode):
        SegmentBase.parse(self, xmlnode)

        self.duration = parse_attr_value(xmlnode, 'duration', int)
        self.start_number = parse_attr_value(xmlnode, 'startNumber', int)
        self.end_number = parse_attr_value(xmlnode, 'endNumber', int)

        self.segment_timelines = parse_child_nodes(xmlnode, 'SegmentTimeline', SegmentTimeline)
        self.bitstream_switchings = parse_child_nodes(xmlnode, 'BitstreamSwitching', URL)

    def write(self, xmlnode):
        SegmentBase.write(self, xmlnode)

        write_attr_value(xmlnode, 'duration', self.duration)
        write_attr_value(xmlnode, 'startNumber', self.start_number)
        write_attr_value(xmlnode, 'endNumber', self.end_number)

        write_child_node(xmlnode, 'SegmentTimeline', self.segment_timelines)
        write_child_node(xmlnode, 'BitstreamSwitching', self.bitstream_switchings)


class SegmentTemplate(MultipleSegmentBase):
    def __init__(self):
        MultipleSegmentBase.__init__(self)

        self.media = None                                     # xs:string
        self.index = None                                     # xs:string
        self.initialization = None                            # xs:string
        self.bitstream_switching = None                       # xs:string

    def parse(self, xmlnode):
        MultipleSegmentBase.parse(self, xmlnode)

        self.media = parse_attr_value(xmlnode, 'media', str)
        self.index = parse_attr_value(xmlnode, 'index', str)
        self.initialization = parse_attr_value(xmlnode, 'initialization', str)
        self.bitstream_switching = parse_attr_value(xmlnode, 'bitstreamSwitching', str)

    def write(self, xmlnode):
        MultipleSegmentBase.write(self, xmlnode)

        write_attr_value(xmlnode, 'media', self.media)
        write_attr_value(xmlnode, 'index', self.index)
        write_attr_value(xmlnode, 'initialization', self.initialization)
        write_attr_value(xmlnode, 'bitstreamSwitching', self.bitstream_switching)


class SegmentList(MultipleSegmentBase):
    def __init__(self):
        MultipleSegmentBase.__init__(self)

        self.segment_urls = None                              # SegmentURLType

    def parse(self, xmlnode):
        MultipleSegmentBase.parse(self, xmlnode)

        self.segment_urls = parse_child_nodes(xmlnode, 'SegmentURL', SegmentURL)

    def write(self, xmlnode):
        MultipleSegmentBase.write(self, xmlnode)

        write_child_node(xmlnode, 'SegmentURL', self.segment_urls)


class Event(XMLNode):
    def __init__(self):
        self.event_value = None                               # xs:string
        self.message_data = None                              # xs:string
        self.presentation_time = None                         # xs:unsignedLong
        self.duration = None                                  # xs:unsignedLong
        self.id = None                                        # xs:unsignedInt

    def parse(self, xmlnode):
        self.event_value = parse_node_value(xmlnode, str)
        self.message_data = parse_attr_value(xmlnode, 'messageData', str)
        self.presentation_time = parse_attr_value(xmlnode, 'presentationTime', int)
        self.duration = parse_attr_value(xmlnode, 'duration', int)
        self.id = parse_attr_value(xmlnode, 'id', int)

    def write(self, xmlnode):
        write_node_value(xmlnode, self.event_value)
        write_attr_value(xmlnode, 'messageData', self.message_data)
        write_attr_value(xmlnode, 'presentationTime', self.presentation_time)
        write_attr_value(xmlnode, 'duration', self.duration)
        write_attr_value(xmlnode, 'id', self.id)


class Descriptor(XMLNode):
    def __init__(self):
        self.scheme_id_uri = ''                               # xs:anyURI (required)
        self.value = None                                     # xs:string
        self.id = None                                        # xs:string

    def parse(self, xmlnode):
        self.scheme_id_uri = parse_attr_value(xmlnode, 'schemeIdUri', str)
        self.value = parse_attr_value(xmlnode, 'value', str)
        self.id = parse_attr_value(xmlnode, 'id', str)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'schemeIdUri', self.scheme_id_uri)
        write_attr_value(xmlnode, 'value', self.value)
        write_attr_value(xmlnode, 'id', self.id)


class PSSH(XMLNode):
    def __init__(self):
        self.pssh = None

    def parse(self, xmlnode):
        self.pssh = parse_node_value(xmlnode, str)

    def write(self, xmlnode):
        write_node_value(xmlnode, self.pssh)


class ContentProtection(XMLNode):
    def __init__(self):
        self.scheme_id_uri = ""                            # xs:anyURI (required)
        self.value = None                                  # xs:string
        self.id = None                                     # xs:string
        self.pssh = None                                   # PSSH
        self.default_key_id = None                         # xs:string
        self.ns2_key_id = None                             # xs:string
        self.cenc_default_kid = None                       # xs:string

    def parse(self, xmlnode):
        self.scheme_id_uri = parse_attr_value(xmlnode, "schemeIdUri", str)
        self.value = parse_attr_value(xmlnode, "value", str)
        self.id = parse_attr_value(xmlnode, "id", str)
        self.default_key_id = parse_attr_value(xmlnode, "default_KID", str)
        self.ns2_key_id = parse_attr_value(xmlnode, "ns2:default_KID", str)
        self.cenc_default_kid = parse_attr_value(xmlnode, "cenc:default_KID", str)
        self.pssh = parse_child_nodes(xmlnode, "cenc:pssh", PSSH)

    def write(self, xmlnode):
        write_attr_value(xmlnode, "schemeIdUri", self.scheme_id_uri)
        write_attr_value(xmlnode, "value", self.value)
        write_attr_value(xmlnode, "id", self.id)
        write_attr_value(xmlnode, "default_KID", self.default_key_id)
        write_attr_value(xmlnode, "ns2:default_KID", self.ns2_key_id)
        write_attr_value(xmlnode, "cenc:default_KID", self.cenc_default_kid)
        write_child_node(xmlnode, "cenc:pssh", self.pssh)


class ContentComponent(XMLNode):
    def __init__(self):
        self.id = None                                        # xs:unsigendInt
        self.lang = None                                      # xs:language
        self.content_type = None                              # xs:string
        self.par = None                                       # RatioType

        self.accessibilities = None                           # DescriptorType*
        self.roles = None                                     # DescriptorType*
        self.ratings = None                                   # DescriptorType*
        self.viewpoints = None                                # DescriptorType*

    def parse(self, xmlnode):
        self.id = parse_attr_value(xmlnode, 'id', int)
        self.lang = parse_attr_value(xmlnode, 'lang', str)
        self.content_type = parse_attr_value(xmlnode, 'contentType', str)
        self.par = parse_attr_value(xmlnode, 'par', str)

        self.accessibilities = parse_child_nodes(xmlnode, 'Accessibility', Descriptor)
        self.roles = parse_child_nodes(xmlnode, 'Role', Descriptor)
        self.ratings = parse_child_nodes(xmlnode, 'Rating', Descriptor)
        self.viewpoints = parse_child_nodes(xmlnode, 'Viewpoint', Descriptor)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'id', self.id)
        write_attr_value(xmlnode, 'lang', self.lang)
        write_attr_value(xmlnode, 'contentType', self.content_type)
        write_attr_value(xmlnode, 'par', self.par)

        write_child_node(xmlnode, 'Accessibility', self.accessibilities)
        write_child_node(xmlnode, 'Role', self.roles)
        write_child_node(xmlnode, 'Rating', self.ratings)
        write_child_node(xmlnode, 'Viewpoint', self.viewpoints)


class RepresentationBase(XMLNode):
    def __init__(self):
        self.profile = None                                   # xs:string
        self.profiles = None                                  # xs:string
        self.width = None                                     # xs:unsigendInt
        self.height = None                                    # xs:unsigendInt
        self.sar = None                                       # RatioType
        self.frame_rate = None                                # FrameRateType
        self.audio_sampling_rate = None                       # xs:string
        self.mime_type = None                                 # xs:string
        self.segment_profiles = None                          # xs:string
        self.codecs = None                                    # xs:string
        self.maximum_sap_period = None                        # xs:double
        self.start_with_sap = None                            # SAPType
        self.max_playout_rate = None                          # xs:double
        self.coding_dependency = None                         # xs:boolean
        self.scan_type = None                                 # VideoScanType

        self.frame_packings = None                            # DescriptorType*
        self.audio_channel_configurations = None              # DescriptorType*
        self.content_protections = None                       # ContentProtection*
        self.essential_properties = None                      # DescriptorType*
        self.supplemental_properties = None                   # DescriptorType*
        self.inband_event_streams = None                      # DescriptorType*

    def parse(self, xmlnode):
        self.profile = parse_attr_value(xmlnode, 'profile', str)
        self.profiles = parse_attr_value(xmlnode, 'profiles', str)
        self.width = parse_attr_value(xmlnode, 'width', int)
        self.height = parse_attr_value(xmlnode, 'height', int)
        self.sar = parse_attr_value(xmlnode, 'sar', str)
        self.frame_rate = parse_attr_value(xmlnode, 'frameRate', str)
        self.audio_sampling_rate = parse_attr_value(xmlnode, 'audioSamplingRate', str)
        self.mime_type = parse_attr_value(xmlnode, 'mimeType', str)
        self.segment_profiles = parse_attr_value(xmlnode, 'segmentProfiles', str)
        self.codecs = parse_attr_value(xmlnode, 'codecs', str)
        self.maximum_sap_period = parse_attr_value(xmlnode, 'maximumSAPPeriod', float)
        self.start_with_sap = parse_attr_value(xmlnode, 'startWithSAP', int)
        self.max_playout_rate = parse_attr_value(xmlnode, 'maxPlayoutRate', float)
        self.coding_dependency = parse_attr_value(xmlnode, 'codingDependency', bool)
        self.scan_type = parse_attr_value(xmlnode, 'scanType', str)

        self.frame_packings = parse_child_nodes(xmlnode, 'FramePacking', Descriptor)
        self.audio_channel_configurations = parse_child_nodes(xmlnode, 'AudioChannelConfiguration', Descriptor)
        self.content_protections = parse_child_nodes(xmlnode, 'ContentProtection', ContentProtection)
        self.essential_properties = parse_child_nodes(xmlnode, 'EssentialProperty', Descriptor)
        self.supplemental_properties = parse_child_nodes(xmlnode, 'SupplementalProperty', Descriptor)
        self.inband_event_streams = parse_child_nodes(xmlnode, 'InbandEventStream', Descriptor)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'profile', self.profile)
        write_attr_value(xmlnode, 'profiles', self.profiles)
        write_attr_value(xmlnode, 'width', self.width)
        write_attr_value(xmlnode, 'height', self.height)
        write_attr_value(xmlnode, 'sar', self.sar)
        write_attr_value(xmlnode, 'frameRate', self.frame_rate)
        write_attr_value(xmlnode, 'audioSamplingRate', self.audio_sampling_rate)
        write_attr_value(xmlnode, 'mimeType', self.mime_type)
        write_attr_value(xmlnode, 'segmentProfiles', self.segment_profiles)
        write_attr_value(xmlnode, 'codecs', self.codecs)
        write_attr_value(xmlnode, 'maximumSAPPeriod', self.maximum_sap_period)
        write_attr_value(xmlnode, 'startWithSAP', self.start_with_sap)
        write_attr_value(xmlnode, 'maxPlayoutRate', self.max_playout_rate)
        write_attr_value(xmlnode, 'codingDependency', self.coding_dependency)
        write_attr_value(xmlnode, 'scanType', self.scan_type)

        write_child_node(xmlnode, 'FramePacking', self.frame_packings)
        write_child_node(xmlnode, 'AudioChannelConfiguration', self.audio_channel_configurations)
        write_child_node(xmlnode, 'ContentProtection', self.content_protections)
        write_child_node(xmlnode, 'EssentialProperty', self.essential_properties)
        write_child_node(xmlnode, 'SupplementalProperty', self.supplemental_properties)
        write_child_node(xmlnode, 'InbandEventStream', self.inband_event_streams)


class Representation(RepresentationBase):
    def __init__(self):
        RepresentationBase.__init__(self)

        self.id = ''                                          # StringNoWhitespaceType (Required)
        self.bandwidth = 0                                    # xs:unsignedInt (required)
        self.quality_ranking = None                           # xs:unsignedInt
        self.dependency_id = None                             # StringVectorType
        self.num_channels = None                              # xs:unsignedInt
        self.sample_rate = None                               # xs:unsignedLong

        self.base_urls = None                                 # BaseURLType*
        self.segment_bases = None                             # SegmentBaseType*
        self.segment_lists = None                             # SegmentListType*
        self.segment_templates = None                         # SegmentTemplateType*
        self.sub_representations = None                       # SubRepresentationType*

    def parse(self, xmlnode):
        RepresentationBase.parse(self, xmlnode)

        self.id = parse_attr_value(xmlnode, 'id', str)
        self.bandwidth = parse_attr_value(xmlnode, 'bandwidth', int)
        self.quality_ranking = parse_attr_value(xmlnode, 'qualityRanking', int)
        self.dependency_id = parse_attr_value(xmlnode, 'dependencyId', [str])
        self.num_channels = parse_attr_value(xmlnode, 'numChannels', int)
        self.sample_rate = parse_attr_value(xmlnode, 'sampleRate', int)

        self.base_urls = parse_child_nodes(xmlnode, 'BaseURL', BaseURL)
        self.segment_bases = parse_child_nodes(xmlnode, 'SegmentBase', SegmentBase)
        self.segment_lists = parse_child_nodes(xmlnode, 'SegmentList', SegmentList)
        self.segment_templates = parse_child_nodes(xmlnode, 'SegmentTemplate', SegmentTemplate)
        self.sub_representations = parse_child_nodes(xmlnode, 'SubRepresentation', SubRepresentation)

    def write(self, xmlnode):
        RepresentationBase.write(self, xmlnode)

        write_attr_value(xmlnode, 'id', self.id)
        write_attr_value(xmlnode, 'width', self.width)
        write_attr_value(xmlnode, 'height', self.height)
        write_attr_value(xmlnode, 'bandwidth', self.bandwidth)
        write_attr_value(xmlnode, 'mimeType', self.mime_type)
        write_attr_value(xmlnode, 'codecs', self.codecs)

        write_child_node(xmlnode, 'BaseURL', self.base_urls)
        write_child_node(xmlnode, 'SegmentBase', self.segment_bases)
        write_child_node(xmlnode, 'SegmentList', self.segment_lists)
        write_child_node(xmlnode, 'SegmentTemplate', self.segment_templates)
        write_child_node(xmlnode, 'SubRepresentation', self.sub_representations)


class SubRepresentation(RepresentationBase):
    def __init__(self):
        RepresentationBase.__init__(self)

        self.level = None                                     # xs:unsigendInt
        self.bandwidth = None                                 # xs:unsignedInt
        self.dependency_level = None                          # UIntVectorType
        self.content_component = None                         # StringVectorType

    def parse(self, xmlnode):
        RepresentationBase.parse(self, xmlnode)

        self.level = parse_attr_value(xmlnode, 'level', int)
        self.bandwidth = parse_attr_value(xmlnode, 'bandwidth', int)
        self.dependency_level = parse_attr_value(xmlnode, 'dependencyLevel', [int])
        self.content_component = parse_attr_value(xmlnode, 'contentComponent', [str])

    def write(self, xmlnode):
        RepresentationBase.write(self, xmlnode)

        write_attr_value(xmlnode, 'level', self.level)
        write_attr_value(xmlnode, 'bandwidth', self.bandwidth)
        write_attr_value(xmlnode, 'dependencyLevel', self.dependency_level)
        write_attr_value(xmlnode, 'contentComponent', self.content_component)


class AdaptationSet(RepresentationBase):
    def __init__(self):
        RepresentationBase.__init__(self)

        self.id = None                                        # xs:unsignedInt
        self.group = None                                     # xs:unsignedInt
        self.lang = None                                      # xs:language
        self.label = None                                     # xs:string
        self.content_type = None                              # xs:string
        self.par = None                                       # RatioType
        self.min_bandwidth = None                             # xs:unsignedInt
        self.max_bandwidth = None                             # xs:unsignedInt
        self.min_width = None                                 # xs:unsignedInt
        self.max_width = None                                 # xs:unsignedInt
        self.min_height = None                                # xs:unsignedInt
        self.max_height = None                                # xs:unsignedInt
        self.min_frame_rate = None                            # FrameRateType
        self.max_frame_rate = None                            # FrameRateType
        self.segment_alignment = None                         # ConditionalUintType
        self.selection_priority = None                        # xs:unsignedInt
        self.subsegment_alignment = None                      # ConditionalUintType
        self.subsegment_starts_with_sap = None                # SAPType
        self.bitstream_switching = None                       # xs:boolean

        self.accessibilities = None                           # DescriptorType*
        self.roles = None                                     # DescriptorType*
        self.ratings = None                                   # DescriptorType*
        self.viewpoints = None                                # DescriptorType*
        self.content_components = None                        # DescriptorType*
        self.base_urls = None                                 # BaseURLType*
        self.segment_bases = None                             # SegmentBase*
        self.segment_lists = None                             # SegmentListType*
        self.segment_templates = None                         # SegmentTemplateType*
        self.representations = None                           # RepresentationType*

    def parse(self, xmlnode):
        RepresentationBase.parse(self, xmlnode)

        self.id = parse_attr_value(xmlnode, 'id', int)
        try:
            self.group = parse_attr_value(xmlnode, 'group', int)
        except ValueError as e:
            # This should never happen, as the specs seem to imply that `group` is always an integer.
            # But there is at least one case (TIDAL) that pushes strings on the manifest, and it's
            # better not to break parsing in that case.
            # See https://github.com/EbbLabs/python-tidal/issues/396
            self.group = parse_attr_value(xmlnode, 'group', str)

        self.lang = parse_attr_value(xmlnode, 'lang', str)
        self.label = parse_attr_value(xmlnode, 'label', str)
        self.content_type = parse_attr_value(xmlnode, 'contentType', str)
        self.par = parse_attr_value(xmlnode, 'par', str)
        self.min_bandwidth = parse_attr_value(xmlnode, 'minBandwidth', int)
        self.max_bandwidth = parse_attr_value(xmlnode, 'maxBandwidth', int)
        self.min_width = parse_attr_value(xmlnode, 'minWidth', int)
        self.max_width = parse_attr_value(xmlnode, 'maxWidth', int)
        self.min_height = parse_attr_value(xmlnode, 'minHeight', int)
        self.max_height = parse_attr_value(xmlnode, 'maxHeight', int)
        self.min_frame_rate = parse_attr_value(xmlnode, 'minFrameRate', str)
        self.max_frame_rate = parse_attr_value(xmlnode, 'maxFrameRate', str)
        self.segment_alignment = parse_attr_value(xmlnode, 'segmentAlignment', bool)
        self.selection_priority = parse_attr_value(xmlnode, 'selectionPriority', int)
        self.subsegment_alignment = parse_attr_value(xmlnode, 'subsegmentAlignment', bool)
        self.subsegment_starts_with_sap = parse_attr_value(xmlnode, 'subsegmentStartsWithSAP', int)
        self.bitstream_switching = parse_attr_value(xmlnode, 'bitstreamSwitching', bool)

        self.accessibilities = parse_child_nodes(xmlnode, 'Accessibility', Descriptor)
        self.roles = parse_child_nodes(xmlnode, 'Role', Descriptor)
        self.ratings = parse_child_nodes(xmlnode, 'Rating', Descriptor)
        self.viewpoints = parse_child_nodes(xmlnode, 'Viewpoint', Descriptor)
        self.content_components = parse_child_nodes(xmlnode, 'ContentComponent', ContentComponent)
        self.base_urls = parse_child_nodes(xmlnode, 'BaseURL', BaseURL)
        self.segment_bases = parse_child_nodes(xmlnode, 'SegmentBase', SegmentBase)
        self.segment_lists = parse_child_nodes(xmlnode, 'SegmentList', SegmentList)
        self.segment_templates = parse_child_nodes(xmlnode, 'SegmentTemplate', SegmentTemplate)
        self.representations = parse_child_nodes(xmlnode, 'Representation', Representation)

    def write(self, xmlnode):
        RepresentationBase.write(self, xmlnode)

        write_attr_value(xmlnode, 'id', self.id)
        write_attr_value(xmlnode, 'group', self.group)
        write_attr_value(xmlnode, 'lang', self.lang)
        write_attr_value(xmlnode, 'label', self.label)
        write_attr_value(xmlnode, 'contentType', self.content_type)
        write_attr_value(xmlnode, 'par', self.par)
        write_attr_value(xmlnode, 'minBandwidth', self.min_bandwidth)
        write_attr_value(xmlnode, 'maxBandwidth', self.max_bandwidth)
        write_attr_value(xmlnode, 'minWidth', self.min_width)
        write_attr_value(xmlnode, 'maxWidth', self.max_width)
        write_attr_value(xmlnode, 'minHeight', self.min_height)
        write_attr_value(xmlnode, 'maxHeight', self.max_height)
        write_attr_value(xmlnode, 'minFrameRate', self.min_frame_rate)
        write_attr_value(xmlnode, 'maxFrameRate', self.max_frame_rate)
        write_attr_value(xmlnode, 'segmentAlignment', self.segment_alignment)
        write_attr_value(xmlnode, 'selectionPriority', self.selection_priority)
        write_attr_value(xmlnode, 'subsegmentAlignment', self.subsegment_alignment)
        write_attr_value(xmlnode, 'subsegmentStartsWithSAP', self.subsegment_starts_with_sap)
        write_attr_value(xmlnode, 'bitstreamSwitching', self.bitstream_switching)

        write_child_node(xmlnode, 'Accessibility', self.accessibilities)
        write_child_node(xmlnode, 'Role', self.roles)
        write_child_node(xmlnode, 'Rating', self.ratings)
        write_child_node(xmlnode, 'Viewpoint', self.viewpoints)
        write_child_node(xmlnode, 'ContentComponent', self.content_components)
        write_child_node(xmlnode, 'BaseURL', self.base_urls)
        write_child_node(xmlnode, 'SegmentBase', self.segment_bases)
        write_child_node(xmlnode, 'SegmentList', self.segment_lists)
        write_child_node(xmlnode, 'SegmentTemplate', self.segment_templates)
        write_child_node(xmlnode, 'Representation', self.representations)


class EventStream(XMLNode):
    def __init__(self):
        self.scheme_id_uri = None                             # xs:anyURI (required)
        self.value = None                                     # xs:string
        self.timescale = None                                 # xs:unsignedInt

        self.events = None                                    # EventType*

    def parse(self, xmlnode):
        self.scheme_id_uri = parse_attr_value(xmlnode, 'schemeIdUri', str)
        self.value = parse_attr_value(xmlnode, 'value', str)
        self.timescale = parse_attr_value(xmlnode, 'timescale', int)

        self.events = parse_child_nodes(xmlnode, 'Event', Event)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'schemeIdUri', self.scheme_id_uri)
        write_attr_value(xmlnode, 'value', self.value)
        write_attr_value(xmlnode, 'timescale', self.timescale)

        write_child_node(xmlnode, 'Event', self.events)


class Period(XMLNode):
    def __init__(self):
        self.id = None                                        # xs:string
        self.start = None                                     # xs:duration
        self.duration = None                                  # xs:duration
        self.bitstream_switching = None                       # xs:boolean

        self.base_urls = None                                 # BaseURLType*
        self.segment_bases = None                             # SegmentBaseType*
        self.segment_lists = None                             # SegmentListType*
        self.segment_templates = None                         # SegmentTemplateType*
        self.asset_identifiers = None                         # DescriptorType*
        self.event_streams = None                             # EventStreamType*
        self.adaptation_sets = None                           # AdaptationSetType*
        self.subsets = None                                   # SubsetType*

    def parse(self, xmlnode):
        self.id = parse_attr_value(xmlnode, 'id', str)
        self.start = parse_attr_value(xmlnode, 'start', str)
        self.duration = parse_attr_value(xmlnode, 'duration', str)
        self.bitstream_switching = parse_attr_value(xmlnode, 'bitstreamSwitching', bool)

        self.base_urls = parse_child_nodes(xmlnode, 'BaseURL', BaseURL)
        self.segment_bases = parse_child_nodes(xmlnode, 'SegmentBase', SegmentBase)
        self.segment_lists = parse_child_nodes(xmlnode, 'SegmentList', SegmentList)
        self.segment_templates = parse_child_nodes(xmlnode, 'SegmentTemplate', SegmentTemplate)
        self.asset_identifiers = parse_child_nodes(xmlnode, 'AssetIdentifier', Descriptor)
        self.event_streams = parse_child_nodes(xmlnode, 'EventStream', EventStream)
        self.adaptation_sets = parse_child_nodes(xmlnode, 'AdaptationSet', AdaptationSet)
        self.subsets = parse_child_nodes(xmlnode, 'Subset', Subset)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'id', self.id)
        write_attr_value(xmlnode, 'start', self.start)
        write_attr_value(xmlnode, 'duration', self.duration)
        write_attr_value(xmlnode, 'bitstreamSwitching', self.bitstream_switching)

        write_child_node(xmlnode, 'BaseURL', self.base_urls)
        write_child_node(xmlnode, 'SegmentBase', self.segment_bases)
        write_child_node(xmlnode, 'SegmentList', self.segment_lists)
        write_child_node(xmlnode, 'SegmentTemplate', self.segment_templates)
        write_child_node(xmlnode, 'AssetIdentifier', self.asset_identifiers)
        write_child_node(xmlnode, 'EventStream', self.event_streams)
        write_child_node(xmlnode, 'AdaptationSet', self.adaptation_sets)
        write_child_node(xmlnode, 'Subset', self.subsets)


class MPEGDASH(XMLNode):
    def __init__(self):
        self.xmlns = None                                     # xmlns
        self.id = None                                        # xs:string
        self.type = None                                      # PresentationType
        self.profiles = ''                                    # xs:string (required)
        self.cenc = None                                      # xs:string
        self.availability_start_time = None                   # xs:dateTime
        self.availability_end_time = None                     # xs:dateTime
        self.publish_time = None                              # xs:dateTime
        self.media_presentation_duration = None               # xs:duration
        self.minimum_update_period = None                     # xs:duration
        self.min_buffer_time = None                           # xs:duration
        self.time_shift_buffer_depth = None                   # xs:duration
        self.suggested_presentation_delay = None              # xs:duration
        self.max_segment_duration = None                      # xs:duration
        self.max_subsegment_duration = None                   # xs:duration

        self.program_informations = None                      # ProgramInformationType*
        self.base_urls = None                                 # BaseURLType*
        self.locations = None                                 # xs:anyURI*
        self.periods = None                                   # PeriodType+
        self.metrics = None                                   # MetricsType*
        self.utc_timings = None                               # DescriptorType*

    def parse(self, xmlnode):
        self.xmlns = parse_attr_value(xmlnode, 'xmlns', str)
        self.id = parse_attr_value(xmlnode, 'id', str)
        self.type = parse_attr_value(xmlnode, 'type', str)
        self.profiles = parse_attr_value(xmlnode, 'profiles', str)
        self.cenc = parse_attr_value(xmlnode, "xmlns:cenc", str)
        self.availability_start_time = parse_attr_value(xmlnode, 'availabilityStartTime', str)
        self.availability_end_time = parse_attr_value(xmlnode, 'availabilityEndTime', str)
        self.publish_time = parse_attr_value(xmlnode, 'publishTime', str)
        self.media_presentation_duration = parse_attr_value(xmlnode, 'mediaPresentationDuration', str)
        self.minimum_update_period = parse_attr_value(xmlnode, 'minimumUpdatePeriod', str)
        self.min_buffer_time = parse_attr_value(xmlnode, 'minBufferTime', str)
        self.time_shift_buffer_depth = parse_attr_value(xmlnode, 'timeShiftBufferDepth', str)
        self.suggested_presentation_delay = parse_attr_value(xmlnode, 'suggestedPresentationDelay', str)
        self.max_segment_duration = parse_attr_value(xmlnode, 'maxSegmentDuration', str)
        self.max_subsegment_duration = parse_attr_value(xmlnode, 'maxSubsegmentDuration', str)

        self.program_informations = parse_child_nodes(xmlnode, 'ProgramInformation', ProgramInformation)
        self.base_urls = parse_child_nodes(xmlnode, 'BaseURL', BaseURL)
        self.locations = parse_child_nodes(xmlnode, 'Location', XsStringElement)
        self.periods = parse_child_nodes(xmlnode, 'Period', Period)
        self.metrics = parse_child_nodes(xmlnode, 'Metrics', Metrics)
        self.utc_timings = parse_child_nodes(xmlnode, 'UTCTiming', Descriptor)

    def write(self, xmlnode):
        write_attr_value(xmlnode, 'xmlns', self.xmlns)
        write_attr_value(xmlnode, 'id', self.id)
        write_attr_value(xmlnode, 'type', self.type)
        write_attr_value(xmlnode, 'profiles', self.profiles)
        write_attr_value(xmlnode, "xmlns:cenc", self.cenc)
        write_attr_value(xmlnode, 'availabilityStartTime', self.availability_start_time)
        write_attr_value(xmlnode, 'availabilityEndTime', self.availability_end_time)
        write_attr_value(xmlnode, 'publishTime', self.publish_time)
        write_attr_value(xmlnode, 'mediaPresentationDuration', self.media_presentation_duration)
        write_attr_value(xmlnode, 'minimumUpdatePeriod', self.minimum_update_period)
        write_attr_value(xmlnode, 'minBufferTime', self.min_buffer_time)
        write_attr_value(xmlnode, 'timeShiftBufferDepth', self.time_shift_buffer_depth)
        write_attr_value(xmlnode, 'suggestedPresentationDelay', self.suggested_presentation_delay)
        write_attr_value(xmlnode, 'maxSegmentDuration', self.max_segment_duration)
        write_attr_value(xmlnode, 'maxSubsegmentDuration', self.max_subsegment_duration)

        write_child_node(xmlnode, 'ProgramInformation', self.program_informations)
        write_child_node(xmlnode, 'BaseURL', self.base_urls)
        write_child_node(xmlnode, 'Location', self.locations)
        write_child_node(xmlnode, 'Period', self.periods)
        write_child_node(xmlnode, 'Metrics', self.metrics)
        write_child_node(xmlnode, 'UTCTiming', self.utc_timings)

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/ObjectPart.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{

  /**
   * <p>A collection of parts associated with a multipart upload.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/GetObjectAttributesParts">AWS
   * API Reference</a></p>
   */
  class GetObjectAttributesParts
  {
  public:
    AWS_S3_API GetObjectAttributesParts();
    AWS_S3_API GetObjectAttributesParts(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API GetObjectAttributesParts& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>The total number of parts.</p>
     */
    inline int GetTotalPartsCount() const{ return m_totalPartsCount; }

    /**
     * <p>The total number of parts.</p>
     */
    inline bool TotalPartsCountHasBeenSet() const { return m_totalPartsCountHasBeenSet; }

    /**
     * <p>The total number of parts.</p>
     */
    inline void SetTotalPartsCount(int value) { m_totalPartsCountHasBeenSet = true; m_totalPartsCount = value; }

    /**
     * <p>The total number of parts.</p>
     */
    inline GetObjectAttributesParts& WithTotalPartsCount(int value) { SetTotalPartsCount(value); return *this;}


    /**
     * <p>The marker for the current part.</p>
     */
    inline int GetPartNumberMarker() const{ return m_partNumberMarker; }

    /**
     * <p>The marker for the current part.</p>
     */
    inline bool PartNumberMarkerHasBeenSet() const { return m_partNumberMarkerHasBeenSet; }

    /**
     * <p>The marker for the current part.</p>
     */
    inline void SetPartNumberMarker(int value) { m_partNumberMarkerHasBeenSet = true; m_partNumberMarker = value; }

    /**
     * <p>The marker for the current part.</p>
     */
    inline GetObjectAttributesParts& WithPartNumberMarker(int value) { SetPartNumberMarker(value); return *this;}


    /**
     * <p>When a list is truncated, this element specifies the last part in the list,
     * as well as the value to use for the <code>PartNumberMarker</code> request
     * parameter in a subsequent request.</p>
     */
    inline int GetNextPartNumberMarker() const{ return m_nextPartNumberMarker; }

    /**
     * <p>When a list is truncated, this element specifies the last part in the list,
     * as well as the value to use for the <code>PartNumberMarker</code> request
     * parameter in a subsequent request.</p>
     */
    inline bool NextPartNumberMarkerHasBeenSet() const { return m_nextPartNumberMarkerHasBeenSet; }

    /**
     * <p>When a list is truncated, this element specifies the last part in the list,
     * as well as the value to use for the <code>PartNumberMarker</code> request
     * parameter in a subsequent request.</p>
     */
    inline void SetNextPartNumberMarker(int value) { m_nextPartNumberMarkerHasBeenSet = true; m_nextPartNumberMarker = value; }

    /**
     * <p>When a list is truncated, this element specifies the last part in the list,
     * as well as the value to use for the <code>PartNumberMarker</code> request
     * parameter in a subsequent request.</p>
     */
    inline GetObjectAttributesParts& WithNextPartNumberMarker(int value) { SetNextPartNumberMarker(value); return *this;}


    /**
     * <p>The maximum number of parts allowed in the response.</p>
     */
    inline int GetMaxParts() const{ return m_maxParts; }

    /**
     * <p>The maximum number of parts allowed in the response.</p>
     */
    inline bool MaxPartsHasBeenSet() const { return m_maxPartsHasBeenSet; }

    /**
     * <p>The maximum number of parts allowed in the response.</p>
     */
    inline void SetMaxParts(int value) { m_maxPartsHasBeenSet = true; m_maxParts = value; }

    /**
     * <p>The maximum number of parts allowed in the response.</p>
     */
    inline GetObjectAttributesParts& WithMaxParts(int value) { SetMaxParts(value); return *this;}


    /**
     * <p>Indicates whether the returned list of parts is truncated. A value of
     * <code>true</code> indicates that the list was truncated. A list can be truncated
     * if the number of parts exceeds the limit returned in the <code>MaxParts</code>
     * element.</p>
     */
    inline bool GetIsTruncated() const{ return m_isTruncated; }

    /**
     * <p>Indicates whether the returned list of parts is truncated. A value of
     * <code>true</code> indicates that the list was truncated. A list can be truncated
     * if the number of parts exceeds the limit returned in the <code>MaxParts</code>
     * element.</p>
     */
    inline bool IsTruncatedHasBeenSet() const { return m_isTruncatedHasBeenSet; }

    /**
     * <p>Indicates whether the returned list of parts is truncated. A value of
     * <code>true</code> indicates that the list was truncated. A list can be truncated
     * if the number of parts exceeds the limit returned in the <code>MaxParts</code>
     * element.</p>
     */
    inline void SetIsTruncated(bool value) { m_isTruncatedHasBeenSet = true; m_isTruncated = value; }

    /**
     * <p>Indicates whether the returned list of parts is truncated. A value of
     * <code>true</code> indicates that the list was truncated. A list can be truncated
     * if the number of parts exceeds the limit returned in the <code>MaxParts</code>
     * element.</p>
     */
    inline GetObjectAttributesParts& WithIsTruncated(bool value) { SetIsTruncated(value); return *this;}


    /**
     * <p>A container for elements related to a particular part. A response can contain
     * zero or more <code>Parts</code> elements.</p>
     */
    inline const Aws::Vector<ObjectPart>& GetParts() const{ return m_parts; }

    /**
     * <p>A container for elements related to a particular part. A response can contain
     * zero or more <code>Parts</code> elements.</p>
     */
    inline bool PartsHasBeenSet() const { return m_partsHasBeenSet; }

    /**
     * <p>A container for elements related to a particular part. A response can contain
     * zero or more <code>Parts</code> elements.</p>
     */
    inline void SetParts(const Aws::Vector<ObjectPart>& value) { m_partsHasBeenSet = true; m_parts = value; }

    /**
     * <p>A container for elements related to a particular part. A response can contain
     * zero or more <code>Parts</code> elements.</p>
     */
    inline void SetParts(Aws::Vector<ObjectPart>&& value) { m_partsHasBeenSet = true; m_parts = std::move(value); }

    /**
     * <p>A container for elements related to a particular part. A response can contain
     * zero or more <code>Parts</code> elements.</p>
     */
    inline GetObjectAttributesParts& WithParts(const Aws::Vector<ObjectPart>& value) { SetParts(value); return *this;}

    /**
     * <p>A container for elements related to a particular part. A response can contain
     * zero or more <code>Parts</code> elements.</p>
     */
    inline GetObjectAttributesParts& WithParts(Aws::Vector<ObjectPart>&& value) { SetParts(std::move(value)); return *this;}

    /**
     * <p>A container for elements related to a particular part. A response can contain
     * zero or more <code>Parts</code> elements.</p>
     */
    inline GetObjectAttributesParts& AddParts(const ObjectPart& value) { m_partsHasBeenSet = true; m_parts.push_back(value); return *this; }

    /**
     * <p>A container for elements related to a particular part. A response can contain
     * zero or more <code>Parts</code> elements.</p>
     */
    inline GetObjectAttributesParts& AddParts(ObjectPart&& value) { m_partsHasBeenSet = true; m_parts.push_back(std::move(value)); return *this; }

  private:

    int m_totalPartsCount;
    bool m_totalPartsCountHasBeenSet = false;

    int m_partNumberMarker;
    bool m_partNumberMarkerHasBeenSet = false;

    int m_nextPartNumberMarker;
    bool m_nextPartNumberMarkerHasBeenSet = false;

    int m_maxParts;
    bool m_maxPartsHasBeenSet = false;

    bool m_isTruncated;
    bool m_isTruncatedHasBeenSet = false;

    Aws::Vector<ObjectPart> m_parts;
    bool m_partsHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws

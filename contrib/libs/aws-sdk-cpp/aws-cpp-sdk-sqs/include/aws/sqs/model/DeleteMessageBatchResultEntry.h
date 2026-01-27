/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sqs/SQS_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
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
namespace SQS
{
namespace Model
{

  /**
   * <p>Encloses the <code>Id</code> of an entry in <code>
   * <a>DeleteMessageBatch</a>.</code> </p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sqs-2012-11-05/DeleteMessageBatchResultEntry">AWS
   * API Reference</a></p>
   */
  class DeleteMessageBatchResultEntry
  {
  public:
    AWS_SQS_API DeleteMessageBatchResultEntry();
    AWS_SQS_API DeleteMessageBatchResultEntry(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_SQS_API DeleteMessageBatchResultEntry& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_SQS_API void OutputToStream(Aws::OStream& ostream, const char* location, unsigned index, const char* locationValue) const;
    AWS_SQS_API void OutputToStream(Aws::OStream& oStream, const char* location) const;


    /**
     * <p>Represents a successfully deleted message.</p>
     */
    inline const Aws::String& GetId() const{ return m_id; }

    /**
     * <p>Represents a successfully deleted message.</p>
     */
    inline bool IdHasBeenSet() const { return m_idHasBeenSet; }

    /**
     * <p>Represents a successfully deleted message.</p>
     */
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    /**
     * <p>Represents a successfully deleted message.</p>
     */
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    /**
     * <p>Represents a successfully deleted message.</p>
     */
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    /**
     * <p>Represents a successfully deleted message.</p>
     */
    inline DeleteMessageBatchResultEntry& WithId(const Aws::String& value) { SetId(value); return *this;}

    /**
     * <p>Represents a successfully deleted message.</p>
     */
    inline DeleteMessageBatchResultEntry& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    /**
     * <p>Represents a successfully deleted message.</p>
     */
    inline DeleteMessageBatchResultEntry& WithId(const char* value) { SetId(value); return *this;}

  private:

    Aws::String m_id;
    bool m_idHasBeenSet = false;
  };

} // namespace Model
} // namespace SQS
} // namespace Aws

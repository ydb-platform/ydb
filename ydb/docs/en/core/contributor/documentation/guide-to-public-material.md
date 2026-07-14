# Adding media to public materials

This article covers placing media on the [Public materials page](../../public-materials/videos/)

## Video

### Title

The first step when publishing video materials is choosing a title. Typically, it is a second-level heading, and the title is exactly the same as the video title on the platform from which the original link came.

### Tag

Each video must have its own tag reflecting the category and brief content. All tag types are described in the repository folder `/public-materials/_includes/tags.md` and on the [page](../../public-materials/videos.md). The tag is placed next after the title.

! [](%E2%9F%A6S1%E2%9F%A7)

### Brief description

The next step is a short one-paragraph description covering only the key points of the video. You don't have to watch the entire video yourself; you can use the video summarization feature with [AI](https://300.ya.ru/), but remember that AI can make mistakes and it's better to double-check the summary yourself.

### Target audience

The next paragraph must definitely be about the target audience of the published video. Consider their specific role ( [application developers](../../dev/index.md), [DevOps engineers](../../dev/index.md), [security engineers](../../security/index.md), etc.), and if you cannot figure it out yourself, ask the talk author. If the author is unavailable, you can ask AI for advice again. It will most likely not be wrong.

### Attaching links

Video links are formatted according to standard md rules. `@[resource_name](link)`. For example:

- `@[youtube](https://youtu.be/Dy0VtzQatag?)`
- `@[rutube](6840af8411a8be4e7da9f82cb4a25103)`
- `@[](https://runtime.strm.yandex.ru/player/episode/vplekzpruxb4htfcgqwm?autoplay=0&mute=1&color=white&rel=0)`
- `@[vk](https://vk.com/video_ext.php?oid=-34475478&id=456239479&hd=2&autoplay=1)`

To get a link from YouTube, just click **Copy video URL** and use the obtained link.

For Rutube, after getting a video link in the format `https://rutube.ru/video/6840af8411a8be4e7da9f82cb4a25103/?r=plwd`, you only need to keep the part with the video ID, as shown in the example above.

For Yandex, just copy the video link; specifying the hosting name in square brackets is optional.

When working with VK video links, note that a playable preview format is only obtained with a link format like `https://vk.com/video_ext.php?oid=-84793390&id=456239888&hd=2&autoplay=0`
Whereas when copying a VK video link, the format will look like this:
`https://vkvideo.ru/video-84793390_456239888`.

In this case, you need to insert the first numeric combination before the underscore into the `oid=` field, and the second combination into the `id=` field.

{% note info %}

If a video is available on multiple resources, you must format links from those resources using tabs `{% list tabs %}`

{% endnote %}

### Adding slides

For some video materials (e.g., conference talks), you need to attach presentation slides placed after the video link in the **Slides** tab.

For correct display, the presentation must be uploaded as a link `https://presentations.ydb.tech/2025/ru/infra_source_craft/presentation.pdf`.

To do this:

- Upload the presentation in PDF format to the [repository](https://github.com/ydb-platform/ydb-presentations).
- Extract the slides link:

! [](%E2%9F%A6S1%E2%9F%A7)

The result will be a link `2025/ru/infra_source_craft/presentation.pdf`.

- Add `https://presentations.ydb.tech/` to the beginning of this link.

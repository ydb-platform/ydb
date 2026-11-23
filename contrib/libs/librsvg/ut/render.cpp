#include <librsvg/rsvg.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cstdint>
#include <memory>

Y_UNIT_TEST_SUITE(TLibrsvg) {
    Y_UNIT_TEST(RenderSvg) {
        constexpr char Svg[] =
            "<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16'>"
            "<rect width='16' height='16' fill='#ff0000'/></svg>";
        GError* error = nullptr;
        std::unique_ptr<RsvgHandle, decltype(&g_object_unref)> handle(
            rsvg_handle_new_from_data(reinterpret_cast<const guint8*>(Svg), sizeof(Svg) - 1, &error),
            g_object_unref);
        UNIT_ASSERT_C(handle, (error ? error->message : "Could not load SVG"));

        std::unique_ptr<cairo_surface_t, decltype(&cairo_surface_destroy)> surface(
            cairo_image_surface_create(CAIRO_FORMAT_ARGB32, 16, 16), cairo_surface_destroy);
        std::unique_ptr<cairo_t, decltype(&cairo_destroy)> context(
            cairo_create(surface.get()), cairo_destroy);
        const RsvgRectangle viewport{0, 0, 16, 16};
        UNIT_ASSERT_C(rsvg_handle_render_document(handle.get(), context.get(), &viewport, &error),
                      (error ? error->message : "Could not render SVG"));
        UNIT_ASSERT_C(cairo_status(context.get()) == CAIRO_STATUS_SUCCESS,
                      cairo_status_to_string(cairo_status(context.get())));
        cairo_surface_flush(surface.get());
        const auto* pixels = reinterpret_cast<const std::uint32_t*>(cairo_image_surface_get_data(surface.get()));
        UNIT_ASSERT_VALUES_EQUAL(pixels[8 * 16 + 8], 0xffff0000u);
    }
}

#include <string.h>
#include <sys/xattr.h>
#include "gsh_list.h"
#include "fsal_api.h"
#include "internal.h"

static int getattr_cb(rgw_xattrlist *attrs, void *arg, uint32_t flags)
{
	rgw_xattrlist *attrs_out = (rgw_xattrlist *)arg;
	rgw_xattr *xattr, *xattr_out;
	int i;

	attrs_out->xattr_cnt = 0;
	for (i = 0; i < attrs->xattr_cnt; i++) {
		xattr = &attrs->xattrs[i];
		xattr_out = gsh_malloc(sizeof(*xattr));

		xattr_out->key.val = xattr->key.val;
		xattr_out->key.len = xattr->key.len;
		xattr_out->val.val = xattr->val.val;
		xattr_out->val.len = xattr->val.len;

		attrs_out->xattrs[attrs_out->xattr_cnt++] = *xattr_out;
	}
	return 0;
}

fsal_status_t rgw_fsal_getxattrs(struct fsal_obj_handle *obj_hdl,
					xattrname4 *xa_name,
					xattrvalue4 *xa_value)
{
	struct rgw_export *export;
	struct rgw_handle *handle;
	rgw_xattrlist xattrlist, xattrlist_out;
	rgw_xattr xattr;
	int rc;

	export = container_of(op_ctx->fsal_export, struct rgw_export, export);
	handle = container_of(obj_hdl, struct rgw_handle, handle);

	xattr.key.val = xa_name->utf8string_val;
	xattr.key.len = xa_name->utf8string_len;
	xattr.val.val = NULL;
	xattr.val.len = 0;
	xattrlist.xattrs = &xattr;
	xattrlist.xattr_cnt = 1;

	rc = rgw_getxattrs(export->rgw_fs, handle->rgw_fh, &xattrlist,
				getattr_cb, &xattrlist_out,
				RGW_GETXATTR_FLAG_NONE);
	if (rc < 0)
		return rgw2fsal_error(rc);

	xa_value->utf8string_val = xattrlist_out.xattrs->val.val;
	xa_value->utf8string_len = xattrlist_out.xattrs->val.len;

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

fsal_status_t rgw_fsal_setxattrs(struct fsal_obj_handle *obj_hdl,
					setxattr_type4 sa_type,
					xattrname4 *xa_name,
					xattrvalue4 *xa_value)
{
	struct rgw_export *export;
	struct rgw_handle *handle;
	rgw_xattrlist xattrlist;
	rgw_xattr xattr;
	int rc;

	export = container_of(op_ctx->fsal_export, struct rgw_export, export);
	handle = container_of(obj_hdl, struct rgw_handle, handle);

	xattr.key.val = xa_name->utf8string_val;
	xattr.key.len = xa_name->utf8string_len;
	xattr.val.val = xa_value->utf8string_val;
	xattr.val.len = xa_value->utf8string_len;
	xattrlist.xattrs = &xattr;
	xattrlist.xattr_cnt = 1;

	rc = rgw_setxattrs(export->rgw_fs, handle->rgw_fh, &xattrlist,
				RGW_SETXATTR_FLAG_NONE);
	if (rc < 0)
		return rgw2fsal_error(rc);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

fsal_status_t rgw_fsal_removexattrs(struct fsal_obj_handle *obj_hdl,
					xattrname4 *xa_name)
{
	struct rgw_export *export;
	struct rgw_handle *handle;
	rgw_xattrlist xattrlist;
	rgw_xattr xattr;
	int rc;

	export = container_of(op_ctx->fsal_export, struct rgw_export, export);
	handle = container_of(obj_hdl, struct rgw_handle, handle);

	xattr.key.val = xa_name->utf8string_val;
	xattr.key.len = xa_name->utf8string_len;
	xattr.val.val = NULL;
	xattr.val.len = 0;
	xattrlist.xattrs = &xattr;
	xattrlist.xattr_cnt = 1;

	rc = rgw_rmxattrs(export->rgw_fs, handle->rgw_fh, &xattrlist,
				RGW_RMXATTR_FLAG_NONE);
	if (rc < 0)
		return rgw2fsal_error(rc);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

fsal_status_t rgw_fsal_listxattrs(struct fsal_obj_handle *obj_hdl,
					count4 la_maxcount,
					nfs_cookie4 *la_cookie,
					verifier4 *la_cookieverf,
					bool_t *lr_eof,
					xattrlist4 *lr_names)
{
	struct rgw_export *export;
	struct rgw_handle *handle;
	rgw_xattrlist xattrlist_out;
	rgw_xattrstr start, filter_prefix;
	component4 *entry;
	int rc, count, i;

	export = container_of(op_ctx->fsal_export, struct rgw_export, export);
	handle = container_of(obj_hdl, struct rgw_handle, handle);

	start.val = NULL;
	start.len = 0;
	filter_prefix.val = NULL;
	filter_prefix.len = 0;

	rc = rgw_lsxattrs(export->rgw_fs, handle->rgw_fh,
				&start, &filter_prefix,
				getattr_cb, &xattrlist_out,
				RGW_LSXATTR_FLAG_NONE);
	if (rc < 0)
		return rgw2fsal_error(rc);

	entry = lr_names->entries;
	count = 0;
	for (i = 0; i < xattrlist_out.xattr_cnt; i++) {
		entry->utf8string_val = xattrlist_out.xattrs[i].key.val;
		entry->utf8string_len = xattrlist_out.xattrs[i].key.len;
		count++;
		entry++;
	}
	lr_names->entryCount = count;
	*lr_eof = true;

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}
